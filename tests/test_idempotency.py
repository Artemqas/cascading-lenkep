import pytest

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
