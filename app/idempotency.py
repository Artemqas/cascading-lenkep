from collections.abc import Callable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment


class IdempotencyConflict(Exception):
    pass


async def get_or_create_payment(
    session: AsyncSession, idempotency_key: str, payload_hash: str, build: Callable[[], Payment]
) -> tuple[Payment, bool]:
    result = await session.execute(select(Payment).where(Payment.idempotency_key == idempotency_key))
    existing = result.scalar_one_or_none()
    if existing is not None:
        if existing.idempotency_payload_hash != payload_hash:
            raise IdempotencyConflict(idempotency_key)
        return existing, False

    payment = build()
    session.add(payment)
    await session.commit()
    return payment, True
