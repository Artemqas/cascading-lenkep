from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.enums import CardType, Direction
from app.models import Provider
from app.providers.base import ProviderAdapter
from app.providers.fast_sync import FastSyncAdapter
from app.providers.flaky import FlakyAdapter
from app.providers.webhook_async import WebhookAsyncAdapter

ADAPTERS: dict[str, ProviderAdapter] = {
    "fast_sync": FastSyncAdapter(),
    "webhook_async": WebhookAsyncAdapter(),
    "flaky": FlakyAdapter(),
}


def get_adapter(provider_id: str) -> ProviderAdapter:
    return ADAPTERS[provider_id]


async def eligible_providers(
    session: AsyncSession, direction: Direction, card_type: CardType, amount: int
) -> list[Provider]:
    result = await session.execute(select(Provider).where(Provider.enabled.is_(True)))
    providers = result.scalars().all()
    return [
        provider
        for provider in providers
        if direction.value in provider.supported_directions
        and card_type.value in provider.supported_card_types
        and provider.min_amount <= amount <= provider.max_amount
    ]
