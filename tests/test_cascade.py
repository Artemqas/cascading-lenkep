import uuid
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from app.cascade import run_cascade
from app.enums import AttemptStatus, CardType, Direction, PaymentStatus
from app.models import Payment, Provider, ProviderAttempt
from app.providers.base import AttemptOutcome, OutcomeKind
from app.providers.fast_sync import FastSyncAdapter
from app.providers.flaky import FlakyAdapter


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


async def test_ambiguous_timeout_is_recorded_as_timed_out_and_cascade_continues(db_session, monkeypatch):
    payment = await seed_payment(db_session, direction=Direction.DEPOSIT, amount=500, card_type=CardType.VISA)
    await seed_provider(db_session, id="fast_sync", priority=1, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    await seed_provider(db_session, id="flaky", priority=2, supported_directions=["deposit"],
                         supported_card_types=["visa"], min_amount=0, max_amount=100000)
    monkeypatch.setattr(FastSyncAdapter, "submit", fake_submit_returning(
        AttemptOutcome(kind=OutcomeKind.AMBIGUOUS)))
    monkeypatch.setattr(FlakyAdapter, "submit", fake_submit_returning(
        AttemptOutcome(kind=OutcomeKind.SUCCESS)))
    monkeypatch.setattr("app.cascade.resolve_ambiguous", AsyncMock(return_value=None))

    await run_cascade(db_session, str(payment.id))

    await db_session.refresh(payment)
    assert payment.status == PaymentStatus.SUCCEEDED
    attempts = await _attempts_for(db_session, payment.id)
    fast_sync_attempt = next(a for a in attempts if a.provider_id == "fast_sync")
    assert fast_sync_attempt.status == AttemptStatus.TIMED_OUT


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
