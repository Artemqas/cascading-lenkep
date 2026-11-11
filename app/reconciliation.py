import asyncio

from app.providers.base import AttemptOutcome, ProviderAdapter
from app.webhooks import event_bus


async def resolve_ambiguous(
    attempt_id: str, provider_reference: str | None, adapter: ProviderAdapter, deadline_seconds: float
) -> AttemptOutcome | None:
    if adapter.supports_status_check:
        return await _poll_status(provider_reference, adapter, deadline_seconds)
    return await _wait_for_webhook(attempt_id, deadline_seconds)


async def _poll_status(
    provider_reference: str | None, adapter: ProviderAdapter, deadline_seconds: float
) -> AttemptOutcome | None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + deadline_seconds
    while True:
        try:
            return await adapter.check_status(provider_reference)
        except asyncio.TimeoutError:
            if loop.time() >= deadline:
                return None
            await asyncio.sleep(0.1)


async def _wait_for_webhook(attempt_id: str, deadline_seconds: float) -> AttemptOutcome | None:
    future = event_bus.register_wait(attempt_id)
    try:
        return await asyncio.wait_for(future, timeout=deadline_seconds)
    except asyncio.TimeoutError:
        return None
