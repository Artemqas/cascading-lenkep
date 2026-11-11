import asyncio
import hashlib
import hmac
import json

import httpx

from app.config import get_settings
from app.models import Payment
from app.providers.base import AttemptOutcome

_RETRY_DELAYS = (0.1, 0.2, 0.4)


class WebhookEventBus:
    def __init__(self) -> None:
        self._waiters: dict[str, asyncio.Future] = {}

    def register_wait(self, attempt_id: str) -> asyncio.Future:
        future = asyncio.get_event_loop().create_future()
        self._waiters[attempt_id] = future
        return future

    def resolve(self, attempt_id: str, outcome: AttemptOutcome) -> bool:
        future = self._waiters.pop(attempt_id, None)
        if future is None or future.done():
            return False
        future.set_result(outcome)
        return True


event_bus = WebhookEventBus()


async def send_merchant_webhook(
    payment: Payment,
    event_type: str,
    extra: dict | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> None:
    body = {
        "event": event_type,
        "payment_id": str(payment.id),
        "status": payment.status.value,
        **(extra or {}),
    }
    raw = json.dumps(body).encode()
    secret = get_settings().merchant_webhook_secret.encode()
    signature = hmac.new(secret, raw, hashlib.sha256).hexdigest()
    headers = {"X-Signature": signature, "Content-Type": "application/json"}

    client = http_client or httpx.AsyncClient()
    try:
        response = await client.post(payment.callback_url, content=raw, headers=headers)
        if response.status_code < 300:
            return
        for delay in _RETRY_DELAYS:
            await asyncio.sleep(delay)
            response = await client.post(payment.callback_url, content=raw, headers=headers)
            if response.status_code < 300:
                return
    finally:
        if http_client is None:
            await client.aclose()
