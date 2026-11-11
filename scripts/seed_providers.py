"""Seeds the three mock providers into the configured database.

Run after `alembic upgrade head`:

    .venv/bin/python scripts/seed_providers.py
"""

import asyncio

from app.db import session_factory
from app.models import Provider


async def main() -> None:
    providers = [
        Provider(
            id="fast_sync", name="fast_sync", priority=None,
            supported_directions=["deposit", "withdraw"],
            supported_card_types=["visa", "mastercard", "other"],
            min_amount=0, max_amount=1_000_000, enabled=True,
        ),
        Provider(
            id="webhook_async", name="webhook_async", priority=None,
            supported_directions=["deposit", "withdraw"],
            supported_card_types=["visa", "mastercard", "other"],
            min_amount=0, max_amount=1_000_000, enabled=True,
        ),
        Provider(
            id="flaky", name="flaky", priority=None,
            supported_directions=["deposit", "withdraw"],
            supported_card_types=["visa", "mastercard", "other"],
            min_amount=0, max_amount=1_000_000, enabled=True,
        ),
    ]
    async with session_factory() as session:
        for provider in providers:
            session.add(provider)
        await session.commit()
    print(f"Seeded {len(providers)} providers.")


if __name__ == "__main__":
    asyncio.run(main())
