import asyncio

from app.providers.base import AttemptOutcome, OutcomeKind
from app.providers.flaky import FlakyAdapter
from app.providers.webhook_async import WebhookAsyncAdapter
from app.reconciliation import resolve_ambiguous
from app.webhooks import event_bus


async def test_resolve_ambiguous_via_status_check_polling():
    adapter = FlakyAdapter()

    async def seed_after_delay():
        await asyncio.sleep(0.05)
        adapter.STATUS_RESOLUTIONS["ref-1"] = AttemptOutcome(kind=OutcomeKind.SUCCESS)

    asyncio.create_task(seed_after_delay())
    outcome = await resolve_ambiguous("attempt-1", "ref-1", adapter, deadline_seconds=1.0)
    assert outcome.kind == OutcomeKind.SUCCESS


async def test_resolve_ambiguous_via_webhook_wait():
    adapter = WebhookAsyncAdapter()

    async def deliver_after_delay():
        await asyncio.sleep(0.05)
        event_bus.resolve("attempt-2", AttemptOutcome(kind=OutcomeKind.DECLINED_RETRYABLE, decline_code="processing_error"))

    asyncio.create_task(deliver_after_delay())
    outcome = await resolve_ambiguous("attempt-2", None, adapter, deadline_seconds=1.0)
    assert outcome.decline_code == "processing_error"


async def test_resolve_ambiguous_returns_none_on_timeout():
    adapter = WebhookAsyncAdapter()
    outcome = await resolve_ambiguous("attempt-3", None, adapter, deadline_seconds=0.05)
    assert outcome is None
