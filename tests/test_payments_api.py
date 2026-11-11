import asyncio
from unittest.mock import AsyncMock

import pytest


@pytest.fixture(autouse=True)
def _mock_merchant_webhook(monkeypatch):
    monkeypatch.setattr("app.cascade.send_merchant_webhook", AsyncMock())


def valid_payment_payload(amount=1000):
    return {
        "direction": "deposit",
        "amount": amount,
        "currency": "USD",
        "card_bin": "411111",
        "card_type": "visa",
        "callback_url": "https://merchant.test/cb",
    }


async def test_post_payments_requires_idempotency_key_header(client):
    resp = await client.post("/payments", json=valid_payment_payload())
    assert resp.status_code == 422


async def test_post_payments_replay_returns_same_payment(client):
    headers = {"Idempotency-Key": "abc"}
    first = await client.post("/payments", json=valid_payment_payload(), headers=headers)
    second = await client.post("/payments", json=valid_payment_payload(), headers=headers)
    assert first.json()["id"] == second.json()["id"]
    assert second.status_code == 200


async def test_post_payments_replay_does_not_spawn_a_second_cascade(client, monkeypatch):
    run_cascade_spy = AsyncMock()
    monkeypatch.setattr("app.api.payments.run_cascade", run_cascade_spy)

    headers = {"Idempotency-Key": "no-double-cascade"}
    await client.post("/payments", json=valid_payment_payload(), headers=headers)
    await asyncio.sleep(0.02)
    await client.post("/payments", json=valid_payment_payload(), headers=headers)
    await asyncio.sleep(0.02)

    assert run_cascade_spy.call_count == 1


async def test_post_payments_same_key_different_body_returns_409(client):
    headers = {"Idempotency-Key": "abc2"}
    await client.post("/payments", json=valid_payment_payload(amount=100), headers=headers)
    resp = await client.post("/payments", json=valid_payment_payload(amount=200), headers=headers)
    assert resp.status_code == 409


async def test_get_payment_returns_attempts(client):
    headers = {"Idempotency-Key": "abc3"}
    created = await client.post("/payments", json=valid_payment_payload(), headers=headers)
    payment_id = created.json()["id"]
    resp = await client.get(f"/payments/{payment_id}")
    assert resp.status_code == 200
    assert "attempts" in resp.json()


async def test_post_payments_rejects_non_positive_amount(client):
    payload = valid_payment_payload(amount=0)
    resp = await client.post("/payments", json=payload, headers={"Idempotency-Key": "neg-amount"})
    assert resp.status_code == 422

    payload = valid_payment_payload(amount=-500)
    resp = await client.post("/payments", json=payload, headers={"Idempotency-Key": "neg-amount-2"})
    assert resp.status_code == 422


async def test_post_payments_rejects_invalid_callback_url(client):
    payload = valid_payment_payload()
    payload["callback_url"] = "not-a-url"
    resp = await client.post("/payments", json=payload, headers={"Idempotency-Key": "bad-url"})
    assert resp.status_code == 422


async def test_scheduled_cascade_task_is_tracked_and_cleaned_up_after_failure(client, monkeypatch):
    # asyncio's own default handler logs unretrieved task exceptions regardless
    # of our code, so logging alone can't tell this fix apart from doing nothing
    # (verified: a vacuous version of this test passed before the fix existed).
    # What the fix actually adds is the strong reference + its cleanup, which is
    # what we assert on here.
    import app.api.payments as payments_module

    monkeypatch.setattr("app.api.payments._run_cascade_background", AsyncMock(side_effect=RuntimeError("boom")))

    resp = await client.post("/payments", json=valid_payment_payload(),
                              headers={"Idempotency-Key": "tracked-background-failure"})
    assert resp.status_code == 202
    assert len(payments_module._background_tasks) == 1

    await asyncio.sleep(0.02)  # let the scheduled task run to completion

    assert len(payments_module._background_tasks) == 0
