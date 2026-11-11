from collections.abc import Callable

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment


class IdempotencyConflict(Exception):
    pass


async def _load_existing(session: AsyncSession, idempotency_key: str) -> Payment | None:
    result = await session.execute(select(Payment).where(Payment.idempotency_key == idempotency_key))
    return result.scalar_one_or_none()


async def get_or_create_payment(
    session: AsyncSession, idempotency_key: str, payload_hash: str, build: Callable[[], Payment]
) -> tuple[Payment, bool]:
    existing = await _load_existing(session, idempotency_key)
    if existing is not None:
        if existing.idempotency_payload_hash != payload_hash:
            raise IdempotencyConflict(idempotency_key)
        return existing, False

    payment = build()
    session.add(payment)
    try:
        await session.commit()
    except IntegrityError:
        # A concurrent request with the same key won the race between our SELECT
        # and our INSERT. Recover by re-reading what it committed, rather than
        # surfacing a 500 for the exact case idempotency exists to serve.
        await session.rollback()
        existing = await _load_existing(session, idempotency_key)
        if existing is None:
            raise
        if existing.idempotency_payload_hash != payload_hash:
            raise IdempotencyConflict(idempotency_key) from None
        return existing, False

    return payment, True
