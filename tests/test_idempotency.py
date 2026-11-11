import pytest

import app.db as db_module
from app.enums import CardType, Direction
from app.idempotency import IdempotencyConflict, get_or_create_payment
from app.models import Payment


async def test_same_idempotency_key_and_payload_returns_original(db_session):
    def build():
        return Payment(direction=Direction.DEPOSIT, amount=100, currency="USD",
                        card_type=CardType.VISA, idempotency_key="k", idempotency_payload_hash="h",
                        callback_url="https://x.test/cb")

    first, created_first = await get_or_create_payment(db_session, "k", "h", build)
    second, created_second = await get_or_create_payment(db_session, "k", "h", build)
    assert created_first is True
    assert created_second is False
    assert first.id == second.id


async def test_same_key_different_payload_raises_conflict(db_session):
    def build():
        return Payment(direction=Direction.DEPOSIT, amount=100, currency="USD",
                        card_type=CardType.VISA, idempotency_key="k", idempotency_payload_hash="h1",
                        callback_url="https://x.test/cb")

    await get_or_create_payment(db_session, "k", "h1", build)
    with pytest.raises(IdempotencyConflict):
        await get_or_create_payment(db_session, "k", "h2", build)


async def test_get_or_create_payment_recovers_from_concurrent_insert_race(db_session, monkeypatch):
    # Simulates two concurrent requests with the same key: both pass the initial
    # SELECT (seeing no row), then race on the INSERT. Here, a competing session
    # commits its row during our own commit() call, so our commit hits the unique
    # constraint — the recovery path must re-select and return the winner's row.
    key = "race-key"
    payload_hash = "h"

    def build():
        return Payment(direction=Direction.DEPOSIT, amount=100, currency="USD",
                        card_type=CardType.VISA, idempotency_key=key,
                        idempotency_payload_hash=payload_hash, callback_url="https://x.test/cb")

    original_commit = db_session.commit
    raced = {"done": False}

    async def commit_with_injected_race():
        if not raced["done"]:
            raced["done"] = True
            async with db_module.session_factory() as other_session:
                other_session.add(build())
                await other_session.commit()
        await original_commit()

    monkeypatch.setattr(db_session, "commit", commit_with_injected_race)

    payment, created = await get_or_create_payment(db_session, key, payload_hash, build)

    assert created is False
    assert payment.idempotency_key == key
