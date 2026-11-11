from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from app.enums import CardType, Direction
from app.models import ProviderStats


def laplace_score(success_count: int, failure_count: int) -> float:
    return (success_count + 1) / (success_count + failure_count + 2)


async def _get_or_create_stats(
    session: AsyncSession, provider_id: str, direction: Direction, card_type: CardType
) -> ProviderStats:
    stats = await session.get(ProviderStats, (provider_id, direction, card_type))
    if stats is None:
        stats = ProviderStats(
            provider_id=provider_id,
            direction=direction,
            card_type=card_type,
            success_count=0,
            failure_count=0,
            updated_at=datetime.now(timezone.utc),
        )
        session.add(stats)
    return stats


async def record_result(
    session: AsyncSession, provider_id: str, direction: Direction, card_type: CardType, success: bool
) -> None:
    stats = await _get_or_create_stats(session, provider_id, direction, card_type)
    if success:
        stats.success_count += 1
    else:
        stats.failure_count += 1
    stats.updated_at = datetime.now(timezone.utc)
    await session.commit()


async def correct_result(
    session: AsyncSession,
    provider_id: str,
    direction: Direction,
    card_type: CardType,
    old_success: bool,
    new_success: bool,
) -> None:
    if old_success == new_success:
        return
    stats = await _get_or_create_stats(session, provider_id, direction, card_type)
    if old_success:
        stats.success_count -= 1
    else:
        stats.failure_count -= 1
    if new_success:
        stats.success_count += 1
    else:
        stats.failure_count += 1
    stats.updated_at = datetime.now(timezone.utc)
    await session.commit()


async def get_score(
    session: AsyncSession, provider_id: str, direction: Direction, card_type: CardType
) -> float:
    stats = await session.get(ProviderStats, (provider_id, direction, card_type))
    if stats is None:
        return laplace_score(0, 0)
    return laplace_score(stats.success_count, stats.failure_count)
