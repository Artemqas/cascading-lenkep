import asyncio
import uuid
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.enums import AttemptStatus, PaymentStatus
from app.models import Payment, ProviderAttempt
from app.providers.base import AttemptOutcome, OutcomeKind, ProviderAttemptRequest
from app.providers.registry import eligible_providers, get_adapter
from app.reconciliation import resolve_ambiguous
from app.routing import order_providers
from app.stats import record_result
from app.webhooks import send_merchant_webhook


def _build_request(attempt: ProviderAttempt, payment: Payment) -> ProviderAttemptRequest:
    scenarios = payment.provider_scenarios or {}
    return ProviderAttemptRequest(
        attempt_id=str(attempt.id),
        direction=payment.direction,
        amount=payment.amount,
        currency=payment.currency,
        card_type=payment.card_type,
        scenario=scenarios.get(attempt.provider_id),
    )


async def run_cascade(session: AsyncSession, payment_id: str) -> None:
    payment = await session.get(Payment, uuid.UUID(payment_id))
    settings = get_settings()

    candidates = await eligible_providers(session, payment.direction, payment.card_type, payment.amount)
    ordered = await order_providers(session, candidates, payment.direction, payment.card_type)
    ordered = ordered[: settings.max_cascade_attempts]

    attempts: list[ProviderAttempt] = []

    for provider in ordered:
        adapter = get_adapter(provider.id)
        attempt = ProviderAttempt(
            id=uuid.uuid4(),
            payment_id=payment.id,
            provider_id=provider.id,
            status=AttemptStatus.PENDING,
            requested_at=datetime.now(timezone.utc),
        )
        session.add(attempt)
        await session.commit()
        attempts.append(attempt)

        request = _build_request(attempt, payment)
        try:
            outcome = await adapter.submit(request)
        except asyncio.TimeoutError:
            outcome = AttemptOutcome(kind=OutcomeKind.AMBIGUOUS)

        if outcome.kind == OutcomeKind.AMBIGUOUS:
            resolved = await resolve_ambiguous(
                str(attempt.id), outcome.provider_reference, adapter, settings.reconciliation_deadline_seconds
            )
            if resolved is None:
                attempt.status = AttemptStatus.TIMED_OUT
                attempt.resolved_at = datetime.now(timezone.utc)
                await record_result(session, provider.id, payment.direction, payment.card_type, success=False)
                await session.commit()
                continue
            outcome = resolved

        attempt.decline_code = outcome.decline_code
        attempt.provider_reference = outcome.provider_reference
        attempt.resolved_at = datetime.now(timezone.utc)

        if outcome.kind == OutcomeKind.SUCCESS:
            attempt.status = AttemptStatus.SUCCEEDED
            await record_result(session, provider.id, payment.direction, payment.card_type, success=True)
            await session.commit()
            await finalize_success(session, payment, attempt)
            return
        elif outcome.kind == OutcomeKind.DECLINED_FINAL:
            attempt.status = AttemptStatus.DECLINED_FINAL
            await record_result(session, provider.id, payment.direction, payment.card_type, success=False)
            await session.commit()
            await finalize_failed(session, payment, attempts)
            return
        else:
            attempt.status = AttemptStatus.DECLINED_RETRYABLE
            await record_result(session, provider.id, payment.direction, payment.card_type, success=False)
            await session.commit()
            continue

    await finalize_failed(session, payment, attempts)


async def finalize_success(session: AsyncSession, payment: Payment, attempt: ProviderAttempt) -> None:
    payment.status = PaymentStatus.SUCCEEDED
    await session.commit()
    await send_merchant_webhook(payment, "payment.succeeded")


async def finalize_failed(session: AsyncSession, payment: Payment, attempts: list[ProviderAttempt]) -> None:
    payment.status = PaymentStatus.FAILED
    await session.commit()
    reasons = [a.decline_code for a in attempts if a.decline_code]
    await send_merchant_webhook(payment, "payment.failed", extra={"reasons": reasons})
