import asyncio
import uuid
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.orm.exc import StaleDataError

import app.db as db_module
from app.cascade import finalize_failed, run_cascade
from app.config import get_settings
from app.enums import AttemptStatus, CardType, Direction, PaymentStatus
from app.models import Payment, Provider, ProviderAttempt
from app.providers.base import AttemptOutcome, OutcomeKind
from app.providers.fast_sync import FastSyncAdapter
from app.providers.flaky import FlakyAdapter
from app.stats import get_score


@pytest.fixture(autouse=True)
def _mock_merchant_webhook(monkeypatch):
    monkeypatch.setattr("app.cascade.send_merchant_webhook", AsyncMock())


async def seed_payment(session, *, direction, amount, card_type):
    payment = Payment(
        direction=direction,
        amount=amount,
        currency="USD",
        card_type=card_type,
        idempotency_key=str(uuid.uuid4()),
        idempotency_payload_hash="h",
        callback_url="https://merchant.test/cb",
    )
    session.add(payment)
    await session.commit()
    return payment


async def seed_provider(session, *, id, priority, supported_directions, supported_card_types,
                         min_amount, max_amount):
    provider = Provider(
        id=id,
        name=id,
        priority=priority,
        supported_directions=supported_directions,
        supported_card_types=supported_card_types,
        min_amount=min_amount,
        max_amount=max_amount,
        enabled=True,
    )
    session.add(provider)
    await session.commit()
    return provider


def fake_submit_returning(outcome: AttemptOutcome):
    async def _submit(self, request):
        return outcome

    return _submit


async def _attempts_for(session, payment_id):
    result = await session.execute(
        select(ProviderAttempt).where(ProviderAttempt.payment_id == payment_id)
    )
    return result.scalars().all()


async def test_final_decline_stops_cascade_without_trying_next_provider(db_session, monkeypatch):
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)
    await seed_provider(db_session, id="fast_sync", priority=1, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    await seed_provider(db_session, id="flaky", priority=2, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    monkeypatch.setattr(FastSyncAdapter, "submit", fake_submit_returning(
        AttemptOutcome(kind=OutcomeKind.DECLINED_FINAL, decline_code="insufficient_funds")))
    flaky_submit = AsyncMock()
    monkeypatch.setattr(FlakyAdapter, "submit", flaky_submit)

    await run_cascade(db_session, str(payment.id))

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.FAILED
    flaky_submit.assert_not_called()


async def test_retryable_decline_cascades_to_next_provider(db_session, monkeypatch):
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)
    await seed_provider(db_session, id="fast_sync", priority=1, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    await seed_provider(db_session, id="flaky", priority=2, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    monkeypatch.setattr(FastSyncAdapter, "submit", fake_submit_returning(
        AttemptOutcome(kind=OutcomeKind.DECLINED_RETRYABLE, decline_code="processing_error")))
    monkeypatch.setattr(FlakyAdapter, "submit", fake_submit_returning(
        AttemptOutcome(kind=OutcomeKind.SUCCESS)))

    await run_cascade(db_session, str(payment.id))

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.SUCCEEDED
    attempts = await _attempts_for(db_session, payment.id)
    assert {a.provider_id for a in attempts} == {"fast_sync", "flaky"}


async def test_ambiguous_timeout_stops_cascade_and_parks_payment_for_review(db_session, monkeypatch):
    # The provider never confirmed or denied the attempt within the deadline —
    # its real outcome is unknown. Trying the next provider here is exactly how
    # a double charge happens (the first provider may have actually succeeded).
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)
    await seed_provider(db_session, id="fast_sync", priority=1, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    await seed_provider(db_session, id="flaky", priority=2, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    monkeypatch.setattr(FastSyncAdapter, "submit", fake_submit_returning(
        AttemptOutcome(kind=OutcomeKind.AMBIGUOUS)))
    flaky_submit = AsyncMock()
    monkeypatch.setattr(FlakyAdapter, "submit", flaky_submit)
    monkeypatch.setattr("app.cascade.resolve_ambiguous", AsyncMock(return_value=None))

    await run_cascade(db_session, str(payment.id))

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.REVIEW_REQUIRED
    flaky_submit.assert_not_called()
    attempts = await _attempts_for(db_session, payment.id)
    assert len(attempts) == 1
    assert attempts[0].status == AttemptStatus.TIMED_OUT


async def test_ambiguous_timeout_does_not_record_provider_stats(db_session, monkeypatch):
    # We don't actually know whether fast_sync succeeded or failed, so adaptive
    # routing must not be biased in either direction off a guess.
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)
    await seed_provider(db_session, id="fast_sync", priority=1, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    monkeypatch.setattr(FastSyncAdapter, "submit", fake_submit_returning(
        AttemptOutcome(kind=OutcomeKind.AMBIGUOUS)))
    monkeypatch.setattr("app.cascade.resolve_ambiguous", AsyncMock(return_value=None))

    await run_cascade(db_session, str(payment.id))

    score = await get_score(db_session, "fast_sync", Direction.DEPOSIT, CardType.VISA)
    assert score == pytest.approx(0.5)  # untouched Laplace default, not nudged toward failure


async def test_concurrent_payment_write_raises_staledataerror(db_session):
    # Sanity check on the optimistic-locking wiring itself, independent of any
    # cascade logic: a write against a stale in-memory copy of `Payment` must
    # fail loudly instead of silently clobbering a concurrent change.
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)

    async with db_module.session_factory() as other_session:
        other_payment = await other_session.get(Payment, payment.id)
        other_payment.status = PaymentStatus.SUCCEEDED
        await other_session.commit()

    payment.status = PaymentStatus.FAILED  # mutate the now-stale in-memory copy, no refresh
    with pytest.raises(StaleDataError):
        await db_session.commit()


async def test_finalize_failed_retries_after_losing_race_to_a_concurrent_conflict(db_session, monkeypatch):
    # Simulates the real race: finalize_failed reads the payment, then — before
    # it commits — a late/contradictory webhook on a different session flags
    # CONFLICT first. finalize_failed must detect the version conflict, re-read,
    # and back off instead of overwriting CONFLICT with FAILED.
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)

    refreshed = asyncio.Event()
    proceed = asyncio.Event()
    original_refresh = db_session.refresh
    call_count = 0

    async def patched_refresh(obj, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        await original_refresh(obj, *args, **kwargs)
        if call_count == 1:
            refreshed.set()
            await proceed.wait()

    monkeypatch.setattr(db_session, "refresh", patched_refresh)

    finalize_task = asyncio.create_task(finalize_failed(db_session, payment, []))
    await refreshed.wait()

    async with db_module.session_factory() as other_session:
        other_payment = await other_session.get(Payment, payment.id)
        other_payment.status = PaymentStatus.CONFLICT
        await other_session.commit()

    proceed.set()
    await finalize_task

    await original_refresh(payment)  # bypass the patch, which only matters during the race
    assert payment.status == PaymentStatus.CONFLICT
    assert call_count == 2  # first read was stale; retried after StaleDataError and saw CONFLICT


async def test_all_providers_exhausted_marks_payment_failed(db_session, monkeypatch):
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)
    await seed_provider(db_session, id="fast_sync", priority=1, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    await seed_provider(db_session, id="flaky", priority=2, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    monkeypatch.setattr(FastSyncAdapter, "submit", fake_submit_returning(
        AttemptOutcome(kind=OutcomeKind.DECLINED_RETRYABLE, decline_code="processing_error")))
    monkeypatch.setattr(FlakyAdapter, "submit", fake_submit_returning(
        AttemptOutcome(kind=OutcomeKind.DECLINED_RETRYABLE, decline_code="processing_error")))

    await run_cascade(db_session, str(payment.id))

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.FAILED
    attempts = await _attempts_for(db_session, payment.id)
    assert len(attempts) == 2


async def test_unexpected_adapter_error_finalizes_as_failed_instead_of_crashing(db_session, monkeypatch, caplog):
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)
    await seed_provider(db_session, id="fast_sync", priority=1, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)

    async def broken_submit(self, request):
        raise ValueError("boom")

    monkeypatch.setattr(FastSyncAdapter, "submit", broken_submit)

    await run_cascade(db_session, str(payment.id))  # must not raise

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.FAILED
    assert "boom" in caplog.text


async def test_payment_is_processing_and_attempt_is_ambiguous_while_awaiting_reconciliation(db_session, monkeypatch):
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)
    await seed_provider(db_session, id="fast_sync", priority=1, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    monkeypatch.setattr(FastSyncAdapter, "submit", fake_submit_returning(AttemptOutcome(kind=OutcomeKind.AMBIGUOUS)))

    hold = asyncio.Event()

    async def blocking_resolve(*args, **kwargs):
        await hold.wait()
        return AttemptOutcome(kind=OutcomeKind.SUCCESS)

    monkeypatch.setattr("app.cascade.resolve_ambiguous", blocking_resolve)

    cascade_task = asyncio.create_task(run_cascade(db_session, str(payment.id)))
    await asyncio.sleep(0.02)

    async with db_module.session_factory() as other_session:
        mid_payment = await other_session.get(Payment, payment.id)
        result = await other_session.execute(
            select(ProviderAttempt).where(ProviderAttempt.payment_id == payment.id)
        )
        mid_attempt = result.scalar_one()
        assert mid_payment.status == PaymentStatus.PROCESSING
        assert mid_attempt.status == AttemptStatus.AMBIGUOUS

    hold.set()
    await cascade_task

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.SUCCEEDED


async def test_ambiguous_timeout_resolves_via_status_check_using_the_attempt_id_as_reference(db_session):
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)
    await seed_provider(db_session, id="flaky", priority=1, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    payment.provider_scenarios = {"flaky": "timeout"}
    await db_session.commit()

    settings = get_settings()
    original_deadline = settings.reconciliation_deadline_seconds
    settings.reconciliation_deadline_seconds = 1.0

    async def seed_resolution_once_attempt_exists():
        for _ in range(50):
            await asyncio.sleep(0.02)
            async with db_module.session_factory() as other_session:
                result = await other_session.execute(
                    select(ProviderAttempt).where(ProviderAttempt.payment_id == payment.id)
                )
                attempt = result.scalar_one_or_none()
            if attempt is not None:
                FlakyAdapter.STATUS_RESOLUTIONS[str(attempt.id)] = AttemptOutcome(kind=OutcomeKind.SUCCESS)
                return

    seeder = asyncio.create_task(seed_resolution_once_attempt_exists())
    try:
        await run_cascade(db_session, str(payment.id))
    finally:
        settings.reconciliation_deadline_seconds = original_deadline
        seeder.cancel()

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.SUCCEEDED


async def test_finalize_does_not_overwrite_an_already_flagged_conflict(db_session):
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)

    # Simulate a late webhook, processed on a *different* request's session,
    # flagging a conflict while this (stale) `payment` object is still in hand.
    async with db_module.session_factory() as other_session:
        other_payment = await other_session.get(Payment, payment.id)
        other_payment.status = PaymentStatus.CONFLICT
        await other_session.commit()

    await finalize_failed(db_session, payment, [])

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.CONFLICT
