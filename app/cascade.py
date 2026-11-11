import asyncio
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app import webhooks as webhooks_module
from app.config import Settings, get_settings
from app.enums import AttemptStatus, PaymentStatus
from app.models import Payment, ProviderAttempt, ReconciliationFlag
from app.providers.base import AttemptOutcome, OutcomeKind, ProviderAttemptRequest
from app.providers.registry import eligible_providers, get_adapter
from app.reconciliation import resolve_ambiguous
from app.routing import order_providers
from app.stats import correct_result, record_result
from app.webhooks import send_merchant_webhook

logger = logging.getLogger(__name__)


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

    payment.status = PaymentStatus.PROCESSING
    await session.commit()

    attempts: list[ProviderAttempt] = []
    try:
        await _run_cascade_loop(session, payment, settings, attempts)
    except Exception:
        # An unexpected error (adapter bug, DB hiccup, etc.) must not leave the
        # payment stuck PROCESSING forever with no merchant notification — treat
        # it as a failure, but make it loud so it's diagnosable.
        logger.exception("run_cascade failed unexpectedly for payment %s", payment_id)
        await session.rollback()
        await finalize_failed(session, payment, attempts)


async def _run_cascade_loop(
    session: AsyncSession, payment: Payment, settings: Settings, attempts: list[ProviderAttempt]
) -> None:
    candidates = await eligible_providers(session, payment.direction, payment.card_type, payment.amount)
    ordered = await order_providers(session, candidates, payment.direction, payment.card_type)
    ordered = ordered[: settings.max_cascade_attempts]

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
            outcome = AttemptOutcome(kind=OutcomeKind.AMBIGUOUS, provider_reference=str(attempt.id))

        if outcome.kind == OutcomeKind.AMBIGUOUS:
            attempt.status = AttemptStatus.AMBIGUOUS
            await session.commit()
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
    await session.refresh(payment)
    if payment.status == PaymentStatus.CONFLICT:
        # A late webhook already flagged a conflict on this payment (processed on
        # a different session) while this cascade was still in flight — don't
        # clobber that with a finalization decided before the conflict existed.
        return
    payment.status = PaymentStatus.SUCCEEDED
    await session.commit()
    await send_merchant_webhook(payment, "payment.succeeded")


async def finalize_failed(session: AsyncSession, payment: Payment, attempts: list[ProviderAttempt]) -> None:
    await session.refresh(payment)
    if payment.status == PaymentStatus.CONFLICT:
        return
    payment.status = PaymentStatus.FAILED
    await session.commit()
    reasons = [a.decline_code for a in attempts if a.decline_code]
    await send_merchant_webhook(payment, "payment.failed", extra={"reasons": reasons})


async def handle_late_resolution(
    session: AsyncSession, attempt: ProviderAttempt, outcome: AttemptOutcome
) -> None:
    payment = await session.get(Payment, attempt.payment_id)

    if outcome.kind != OutcomeKind.SUCCESS:
        # A late (re-)confirmation of a decline. The cascade already counted this
        # attempt as a failure when it moved past it, so there's nothing to correct.
        return

    result = await session.execute(
        select(ProviderAttempt).where(
            ProviderAttempt.payment_id == payment.id,
            ProviderAttempt.status == AttemptStatus.SUCCEEDED,
        )
    )
    succeeded_attempts = result.scalars().all()

    if any(a.id == attempt.id for a in succeeded_attempts):
        return  # duplicate webhook delivery for an attempt that already succeeded

    # This attempt isn't among the succeeded ones — either nothing has succeeded
    # yet (payment was finalized FAILED) or a *different* attempt succeeded.
    # Either way this late SUCCESS contradicts what the cascade already finalized.
    payment.status = PaymentStatus.CONFLICT
    attempt_ids = [str(attempt.id)] + [str(a.id) for a in succeeded_attempts]
    flag = ReconciliationFlag(
        id=uuid.uuid4(),
        payment_id=payment.id,
        reason="late_success_after_finalization",
        attempt_ids=attempt_ids,
        created_at=datetime.now(timezone.utc),
    )
    session.add(flag)
    await correct_result(session, attempt.provider_id, payment.direction, payment.card_type,
                          old_success=False, new_success=True)
    attempt.status = AttemptStatus.SUCCEEDED
    attempt.resolved_at = datetime.now(timezone.utc)
    await session.commit()
    await webhooks_module.send_merchant_webhook(payment, "payment.conflict_detected")
