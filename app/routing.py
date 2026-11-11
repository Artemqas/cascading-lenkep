from sqlalchemy.ext.asyncio import AsyncSession

from app.enums import CardType, Direction
from app.models import Provider
from app.stats import get_score


async def order_providers(
    session: AsyncSession, eligible: list[Provider], direction: Direction, card_type: CardType
) -> list[Provider]:
    manual = sorted((p for p in eligible if p.priority is not None), key=lambda p: p.priority)
    adaptive_pool = [p for p in eligible if p.priority is None]
    scored = [(p, await get_score(session, p.id, direction, card_type)) for p in adaptive_pool]
    adaptive_sorted = [p for p, _ in sorted(scored, key=lambda t: t[1], reverse=True)]
    return manual + adaptive_sorted
