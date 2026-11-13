import asyncio
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import StaleDataError

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

_MAX_VERSION_CONFLICT_RETRIES = 3


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
                # The provider never confirmed or denied this attempt — its real
                # outcome is unknown, not "probably failed". Trying the next
                # provider here is exactly how a double charge happens. Stop and
                # park the payment for a human to reconcile with the provider
                # directly; stats stay untouched since we don't actually know
                # whether this provider succeeded or failed.
                attempt.status = AttemptStatus.TIMED_OUT
                attempt.resolved_at = datetime.now(timezone.utc)
                await session.commit()
                await mark_review_required(session, payment)
                return
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


async def _retry_on_stale_payment(session: AsyncSession, payment: Payment, mutate) -> bool:
    """Reloads `payment`, calls `mutate(payment)` to decide-and-apply a change in
    place (return True to commit, False to skip), and retries the whole
    reread-decide-write cycle if a concurrent writer (the cascade vs. a late/
    contradictory webhook, both targeting the same `Payment` row) commits first
    and bumps `Payment.version` out from under us."""
    for _ in range(_MAX_VERSION_CONFLICT_RETRIES):
        await session.refresh(payment)
        if not mutate(payment):
            return False
        try:
            await session.commit()
            return True
        except StaleDataError:
            await session.rollback()
    raise RuntimeError(f"version conflict retries exhausted for payment {payment.id}")


async def finalize_success(session: AsyncSession, payment: Payment, attempt: ProviderAttempt) -> None:
    def mutate(p: Payment) -> bool:
        if p.status == PaymentStatus.CONFLICT:
            # A late webhook already flagged a conflict on this payment (processed
            # on a different session) while this cascade was still in flight —
            # don't clobber that with a finalization decided before it existed.
            return False
        p.status = PaymentStatus.SUCCEEDED
        return True

    if await _retry_on_stale_payment(session, payment, mutate):
        await send_merchant_webhook(payment, "payment.succeeded")


async def finalize_failed(session: AsyncSession, payment: Payment, attempts: list[ProviderAttempt]) -> None:
    def mutate(p: Payment) -> bool:
        if p.status == PaymentStatus.CONFLICT:
            return False
        p.status = PaymentStatus.FAILED
        return True

    if await _retry_on_stale_payment(session, payment, mutate):
        reasons = [a.decline_code for a in attempts if a.decline_code]
        await send_merchant_webhook(payment, "payment.failed", extra={"reasons": reasons})


async def mark_review_required(session: AsyncSession, payment: Payment) -> None:
    def mutate(p: Payment) -> bool:
        if p.status == PaymentStatus.CONFLICT:
            return False
        p.status = PaymentStatus.REVIEW_REQUIRED
        return True

    await _retry_on_stale_payment(session, payment, mutate)
    logger.warning(
        "payment %s parked for manual review: provider never confirmed or denied the attempt",
        payment.id,
    )


async def handle_late_resolution(
    session: AsyncSession, attempt: ProviderAttempt, outcome: AttemptOutcome
) -> None:
    if attempt.status == AttemptStatus.SUCCEEDED:
        return  # duplicate webhook delivery for an attempt that already succeeded

    payment = await session.get(Payment, attempt.payment_id)
    await session.refresh(payment)

    if attempt.status in (AttemptStatus.AMBIGUOUS, AttemptStatus.TIMED_OUT) and payment.status in (
        PaymentStatus.PROCESSING,
        PaymentStatus.REVIEW_REQUIRED,
    ):
        # This is the attempt the cascade gave up waiting on (or is about to) and
        # parked for manual review. Record what the provider actually said — a
        # tech-support engineer needs this in the log to reconcile with the
        # provider — but the payment's fate stays a human decision from here,
        # never auto-applied.
        attempt.decline_code = outcome.decline_code
        attempt.provider_reference = outcome.provider_reference
        attempt.resolved_at = datetime.now(timezone.utc)
        logger.info(
            "late webhook for review-required payment %s (attempt %s): %s",
            payment.id, attempt.id, outcome.kind,
        )
        await session.commit()
        return

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
    attempt_ids = [str(attempt.id)] + [str(a.id) for a in succeeded_attempts]

    def mutate(p: Payment) -> bool:
        p.status = PaymentStatus.CONFLICT
        return True

    await _retry_on_stale_payment(session, payment, mutate)
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
