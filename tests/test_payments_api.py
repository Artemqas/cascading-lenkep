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
