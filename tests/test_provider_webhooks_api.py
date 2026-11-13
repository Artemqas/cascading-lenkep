import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock

from sqlalchemy import select

from app.cascade import handle_late_resolution
from app.enums import AttemptStatus, CardType, Direction, PaymentStatus
from app.models import Payment, ProviderAttempt, ReconciliationFlag
from app.providers.base import AttemptOutcome, OutcomeKind


async def seed_succeeded_payment(session, *, via_provider):
    payment = Payment(
        direction=Direction.DEPOSIT, amount=1000, currency="USD", card_type=CardType.VISA,
        status=PaymentStatus.SUCCEEDED, idempotency_key=str(uuid.uuid4()),
        idempotency_payload_hash="h", callback_url="https://merchant.test/cb",
    )
    session.add(payment)
    await session.flush()
    attempt = ProviderAttempt(
        id=uuid.uuid4(), payment_id=payment.id, provider_id=via_provider,
        status=AttemptStatus.SUCCEEDED, requested_at=datetime.now(timezone.utc),
        resolved_at=datetime.now(timezone.utc),
    )
    session.add(attempt)
    await session.commit()
    return payment


async def get_succeeded_attempt(session, payment_id):
    result = await session.execute(
        select(ProviderAttempt).where(
            ProviderAttempt.payment_id == payment_id, ProviderAttempt.status == AttemptStatus.SUCCEEDED
        )
    )
    return result.scalar_one()


async def seed_timed_out_attempt(session, payment_id, *, provider_id):
    attempt = ProviderAttempt(
        id=uuid.uuid4(), payment_id=payment_id, provider_id=provider_id,
        status=AttemptStatus.TIMED_OUT, requested_at=datetime.now(timezone.utc),
        resolved_at=datetime.now(timezone.utc),
    )
    session.add(attempt)
    await session.commit()
    return attempt


async def seed_review_required_payment_with_timed_out_attempt(session, *, provider_id):
    payment = Payment(
        direction=Direction.DEPOSIT, amount=1000, currency="USD", card_type=CardType.VISA,
        status=PaymentStatus.REVIEW_REQUIRED, idempotency_key=str(uuid.uuid4()),
        idempotency_payload_hash="h", callback_url="https://merchant.test/cb",
    )
    session.add(payment)
    await session.flush()
    attempt = ProviderAttempt(
        id=uuid.uuid4(), payment_id=payment.id, provider_id=provider_id,
        status=AttemptStatus.TIMED_OUT, requested_at=datetime.now(timezone.utc),
        resolved_at=datetime.now(timezone.utc),
    )
    session.add(attempt)
    await session.commit()
    return payment, attempt


async def load_reconciliation_flags(session, payment_id):
    result = await session.execute(
        select(ReconciliationFlag).where(ReconciliationFlag.payment_id == payment_id)
    )
    return result.scalars().all()


async def test_duplicate_webhook_matching_recorded_outcome_is_noop(db_session, monkeypatch):
    payment = await seed_succeeded_payment(db_session, via_provider="webhook_async")
    attempt = await get_succeeded_attempt(db_session, payment.id)
    send_webhook_spy = AsyncMock()
    monkeypatch.setattr("app.webhooks.send_merchant_webhook", send_webhook_spy)
    correct_result_spy = AsyncMock()
    monkeypatch.setattr("app.cascade.correct_result", correct_result_spy)

    await handle_late_resolution(db_session, attempt, AttemptOutcome(kind=OutcomeKind.SUCCESS))

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.SUCCEEDED
    send_webhook_spy.assert_not_called()
    correct_result_spy.assert_not_called()


async def test_late_success_on_different_attempt_after_finalization_is_conflict(db_session, monkeypatch):
    payment = await seed_succeeded_payment(db_session, via_provider="flaky")
    timed_out_attempt = await seed_timed_out_attempt(db_session, payment.id, provider_id="webhook_async")
    send_webhook_spy = AsyncMock()
    monkeypatch.setattr("app.webhooks.send_merchant_webhook", send_webhook_spy)

    await handle_late_resolution(db_session, timed_out_attempt, AttemptOutcome(kind=OutcomeKind.SUCCESS))

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.CONFLICT
    flags = await load_reconciliation_flags(db_session, payment.id)
    assert len(flags) == 1
    send_webhook_spy.assert_called_once_with(payment, "payment.conflict_detected")


async def test_late_webhook_for_review_required_payment_logs_success_without_changing_status(
    db_session, monkeypatch
):
    payment, attempt = await seed_review_required_payment_with_timed_out_attempt(
        db_session, provider_id="webhook_async")
    send_webhook_spy = AsyncMock()
    monkeypatch.setattr("app.webhooks.send_merchant_webhook", send_webhook_spy)
    correct_result_spy = AsyncMock()
    monkeypatch.setattr("app.cascade.correct_result", correct_result_spy)

    await handle_late_resolution(db_session, attempt, AttemptOutcome(kind=OutcomeKind.SUCCESS))

    await db_session.refresh(payment)
    await db_session.refresh(attempt)
    assert payment.status == PaymentStatus.REVIEW_REQUIRED  # a human decides from here, not this call
    assert attempt.decline_code is None
    assert attempt.resolved_at is not None
    send_webhook_spy.assert_not_called()
    correct_result_spy.assert_not_called()
    flags = await load_reconciliation_flags(db_session, payment.id)
    assert flags == []


async def test_late_webhook_for_review_required_payment_logs_decline_without_changing_status(
    db_session, monkeypatch
):
    payment, attempt = await seed_review_required_payment_with_timed_out_attempt(
        db_session, provider_id="webhook_async")
    send_webhook_spy = AsyncMock()
    monkeypatch.setattr("app.webhooks.send_merchant_webhook", send_webhook_spy)

    await handle_late_resolution(
        db_session, attempt, AttemptOutcome(kind=OutcomeKind.DECLINED_RETRYABLE, decline_code="processing_error")
    )

    await db_session.refresh(payment)
    await db_session.refresh(attempt)
    assert payment.status == PaymentStatus.REVIEW_REQUIRED
    assert attempt.decline_code == "processing_error"
    send_webhook_spy.assert_not_called()


async def test_provider_webhook_endpoint_returns_200_even_for_unknown_attempt(client):
    resp = await client.post("/webhooks/providers/webhook_async",
                              json={"attempt_id": "does-not-exist", "status": "approved"})
    assert resp.status_code == 200


async def test_redelivered_late_webhook_after_conflict_is_still_a_noop(db_session, monkeypatch):
    payment = await seed_succeeded_payment(db_session, via_provider="flaky")
    timed_out_attempt = await seed_timed_out_attempt(db_session, payment.id, provider_id="webhook_async")
    monkeypatch.setattr("app.webhooks.send_merchant_webhook", AsyncMock())
    correct_result_spy = AsyncMock()
    monkeypatch.setattr("app.cascade.correct_result", correct_result_spy)

    await handle_late_resolution(db_session, timed_out_attempt, AttemptOutcome(kind=OutcomeKind.SUCCESS))
    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.CONFLICT
    correct_result_spy.assert_called_once()

    # Re-delivery of the exact same late webhook must not crash, and must not
    # double-count the stats correction — there are now *two* SUCCEEDED attempts
    # on this payment (the original and the late one).
    await handle_late_resolution(db_session, timed_out_attempt, AttemptOutcome(kind=OutcomeKind.SUCCESS))
    correct_result_spy.assert_called_once()  # still just the one call from the first delivery

    flags = await load_reconciliation_flags(db_session, payment.id)
    assert len(flags) == 1  # no second flag from the redelivery


async def test_provider_webhook_endpoint_returns_200_for_unknown_provider_id(client):
    resp = await client.post("/webhooks/providers/does_not_exist",
                              json={"attempt_id": "x", "status": "approved"})
    assert resp.status_code == 200


async def test_provider_webhook_endpoint_returns_200_for_malformed_body(client):
    resp = await client.post("/webhooks/providers/webhook_async", json={})
    assert resp.status_code == 200


async def test_provider_webhook_endpoint_returns_200_for_provider_without_webhook_support(client):
    # fast_sync and flaky resolve via other mechanisms; their parse_webhook raises
    # NotImplementedError. A real provider calling the wrong path must still get 200.
    resp = await client.post("/webhooks/providers/fast_sync",
                              json={"attempt_id": "x", "status": "approved"})
    assert resp.status_code == 200
