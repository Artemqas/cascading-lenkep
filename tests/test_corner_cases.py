import asyncio

import httpx
import pytest
from sqlalchemy import select

from app.config import get_settings
from app.models import Provider, ProviderStats


@pytest.fixture(autouse=True)
def _mock_merchant_endpoint(respx_mock):
    # respx intercepts at the transport level, so this covers every call site
    # that sends the merchant webhook (finalize_success/finalize_failed in
    # app.cascade, and handle_late_resolution's app.webhooks-qualified call)
    # without caring which module's imported name was used.
    respx_mock.post("https://merchant.test/cb").mock(return_value=httpx.Response(200))


def payment_payload(*, direction="deposit", amount=1000, currency="USD", card_bin="411111",
                     card_type="visa", callback_url="https://merchant.test/cb",
                     provider_scenarios=None):
    return {
        "direction": direction,
        "amount": amount,
        "currency": currency,
        "card_bin": card_bin,
        "card_type": card_type,
        "callback_url": callback_url,
        "provider_scenarios": provider_scenarios,
    }


async def _wait_for_terminal_status(client, payment_id, timeout=2.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        resp = await client.get(f"/payments/{payment_id}")
        body = resp.json()
        if body["status"] not in ("pending", "processing"):
            return body
        if asyncio.get_event_loop().time() >= deadline:
            return body
        await asyncio.sleep(0.02)


async def _set_priority(db_session, provider_id, priority):
    result = await db_session.execute(select(Provider).where(Provider.id == provider_id))
    provider = result.scalar_one()
    provider.priority = priority
    await db_session.commit()


async def _set_enabled(db_session, provider_id, enabled):
    result = await db_session.execute(select(Provider).where(Provider.id == provider_id))
    provider = result.scalar_one()
    provider.enabled = enabled
    await db_session.commit()


async def _seed_stats(db_session, provider_id, *, success_count, failure_count):
    from datetime import datetime, timezone

    from app.enums import CardType, Direction

    db_session.add(ProviderStats(
        provider_id=provider_id, direction=Direction.DEPOSIT, card_type=CardType.VISA,
        success_count=success_count, failure_count=failure_count,
        updated_at=datetime.now(timezone.utc),
    ))
    await db_session.commit()


async def test_e2e_manual_priority_overrides_adaptive_history(client, db_session, seed_providers):
    # fast_sync has a near-perfect adaptive history, but flaky has a manual
    # priority — manual priority must win even though adaptive would favor fast_sync.
    await _seed_stats(db_session, "fast_sync", success_count=100, failure_count=0)
    await _set_priority(db_session, "flaky", 1)

    resp = await client.post("/payments", json=payment_payload(
        provider_scenarios={"fast_sync": "insufficient_funds"}),
        headers={"Idempotency-Key": "priority-1"})
    payment_id = resp.json()["id"]

    body = await _wait_for_terminal_status(client, payment_id)
    assert body["status"] == "succeeded"
    assert len(body["attempts"]) == 1
    assert body["attempts"][0]["provider_id"] == "flaky"


async def test_e2e_final_decline_does_not_cascade(client, db_session, seed_providers):
    await _set_priority(db_session, "fast_sync", 1)

    resp = await client.post("/payments", json=payment_payload(
        provider_scenarios={"fast_sync": "fraud_suspected"}),
        headers={"Idempotency-Key": "final-decline-1"})
    payment_id = resp.json()["id"]

    body = await _wait_for_terminal_status(client, payment_id)
    assert body["status"] == "failed"
    assert len(body["attempts"]) == 1
    assert body["attempts"][0]["provider_id"] == "fast_sync"


async def test_e2e_retryable_decline_cascades_to_next_provider(client, db_session, seed_providers):
    await _set_priority(db_session, "fast_sync", 1)
    await _set_priority(db_session, "flaky", 2)

    resp = await client.post("/payments", json=payment_payload(
        provider_scenarios={"fast_sync": "processing_error"}),
        headers={"Idempotency-Key": "retryable-1"})
    payment_id = resp.json()["id"]

    body = await _wait_for_terminal_status(client, payment_id)
    assert body["status"] == "succeeded"
    assert {a["provider_id"] for a in body["attempts"]} == {"fast_sync", "flaky"}


async def test_e2e_all_providers_exhausted_returns_failed(client, db_session, seed_providers):
    await _set_priority(db_session, "fast_sync", 1)
    await _set_priority(db_session, "flaky", 2)
    await _set_enabled(db_session, "webhook_async", False)

    resp = await client.post("/payments", json=payment_payload(
        provider_scenarios={"fast_sync": "processing_error", "flaky": "declined_via_body"}),
        headers={"Idempotency-Key": "exhausted-1"})
    payment_id = resp.json()["id"]

    body = await _wait_for_terminal_status(client, payment_id)
    assert body["status"] == "failed"
    assert {a["provider_id"] for a in body["attempts"]} == {"fast_sync", "flaky"}


async def test_e2e_duplicate_post_with_same_idempotency_key_is_noop(client, seed_providers):
    headers = {"Idempotency-Key": "dup-post-1"}
    # Amount above every seeded provider's max_amount: zero eligible providers,
    # so the cascade finalizes instantly — this test only cares about the
    # idempotent-create behavior, not cascade outcome.
    payload = payment_payload(amount=10_000_000)

    first = await client.post("/payments", json=payload, headers=headers)
    second = await client.post("/payments", json=payload, headers=headers)

    assert first.status_code == 202
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]


async def test_e2e_duplicate_webhook_delivery_is_noop(client, db_session, seed_providers):
    await _set_priority(db_session, "webhook_async", 1)

    resp = await client.post("/payments", json=payment_payload(),
                              headers={"Idempotency-Key": "dup-webhook-1"})
    payment_id = resp.json()["id"]

    # Let the cascade create the webhook_async attempt and start waiting on the webhook.
    await asyncio.sleep(0.05)
    detail = (await client.get(f"/payments/{payment_id}")).json()
    attempt = next(a for a in detail["attempts"] if a["provider_id"] == "webhook_async")

    webhook_payload = {"attempt_id": attempt["id"], "status": "approved"}
    first = await client.post("/webhooks/providers/webhook_async", json=webhook_payload)
    assert first.status_code == 200

    body = await _wait_for_terminal_status(client, payment_id)
    assert body["status"] == "succeeded"

    second = await client.post("/webhooks/providers/webhook_async", json=webhook_payload)
    assert second.status_code == 200

    after = await client.get(f"/payments/{payment_id}")
    assert after.json()["status"] == "succeeded"


async def test_e2e_late_webhook_after_cascade_moved_on_flags_conflict(client, db_session, seed_providers):
    settings = get_settings()
    original_deadline = settings.reconciliation_deadline_seconds
    settings.reconciliation_deadline_seconds = 0.2
    try:
        await _set_priority(db_session, "webhook_async", 1)
        await _set_priority(db_session, "fast_sync", 2)

        resp = await client.post("/payments", json=payment_payload(
            provider_scenarios={"fast_sync": "success"}),
            headers={"Idempotency-Key": "late-webhook-1"})
        payment_id = resp.json()["id"]

        body = await _wait_for_terminal_status(client, payment_id, timeout=2.0)
        assert body["status"] == "succeeded"
        timed_out_attempt = next(a for a in body["attempts"] if a["provider_id"] == "webhook_async")
        assert timed_out_attempt["status"] == "timed_out"

        late_payload = {"attempt_id": timed_out_attempt["id"], "status": "approved"}
        resp = await client.post("/webhooks/providers/webhook_async", json=late_payload)
        assert resp.status_code == 200

        final = await client.get(f"/payments/{payment_id}")
        assert final.json()["status"] == "conflict"
    finally:
        settings.reconciliation_deadline_seconds = original_deadline


async def test_e2e_adaptive_routing_prefers_provider_with_better_history(client, db_session, seed_providers):
    await _seed_stats(db_session, "fast_sync", success_count=10, failure_count=0)
    await _seed_stats(db_session, "flaky", success_count=0, failure_count=10)

    resp = await client.post("/payments", json=payment_payload(
        provider_scenarios={"fast_sync": "success", "flaky": "declined_via_http"}),
        headers={"Idempotency-Key": "adaptive-1"})
    payment_id = resp.json()["id"]

    body = await _wait_for_terminal_status(client, payment_id)
    assert body["status"] == "succeeded"
    assert len(body["attempts"]) == 1
    assert body["attempts"][0]["provider_id"] == "fast_sync"
