from app.enums import CardType, Direction, PaymentStatus
from app.models import Payment


async def test_can_persist_and_load_payment(db_session):
    payment = Payment(direction=Direction.DEPOSIT, amount=1000, currency="USD",
                       card_type=CardType.VISA, idempotency_key="k1",
                       idempotency_payload_hash="h1", callback_url="https://merchant.test/cb")
    db_session.add(payment)
    await db_session.commit()
    loaded = await db_session.get(Payment, payment.id)
    assert loaded.status == PaymentStatus.PENDING
