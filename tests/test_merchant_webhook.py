import json
import uuid

import httpx

from app.enums import CardType, Direction, PaymentStatus
from app.models import Payment
from app.webhooks import send_merchant_webhook


def make_payment(*, callback_url, status):
    return Payment(
        id=uuid.uuid4(),
        direction=Direction.DEPOSIT,
        amount=1000,
        currency="USD",
        card_type=CardType.VISA,
        status=status,
        idempotency_key=str(uuid.uuid4()),
        idempotency_payload_hash="h",
        callback_url=callback_url,
    )


async def test_send_merchant_webhook_signs_and_posts(respx_mock):
    route = respx_mock.post("https://merchant.test/cb").mock(return_value=httpx.Response(200))
    payment = make_payment(callback_url="https://merchant.test/cb", status=PaymentStatus.SUCCEEDED)
    await send_merchant_webhook(payment, "payment.succeeded")
    assert route.called
    sent_body = json.loads(route.calls[0].request.content)
    assert sent_body["event"] == "payment.succeeded"
    assert "X-Signature" in route.calls[0].request.headers


async def test_send_merchant_webhook_retries_on_failure(respx_mock):
    route = respx_mock.post("https://merchant.test/cb").mock(
        side_effect=[httpx.Response(500), httpx.Response(200)])
    payment = make_payment(callback_url="https://merchant.test/cb", status=PaymentStatus.FAILED)
    await send_merchant_webhook(payment, "payment.failed")
    assert route.call_count == 2


async def test_send_merchant_webhook_retries_on_connection_error(respx_mock):
    # A ConnectError/ReadTimeout *is* non-delivery — the thing retries exist for —
    # not just a non-2xx response.
    route = respx_mock.post("https://merchant.test/cb").mock(
        side_effect=[httpx.ConnectError("boom"), httpx.Response(200)])
    payment = make_payment(callback_url="https://merchant.test/cb", status=PaymentStatus.SUCCEEDED)
    await send_merchant_webhook(payment, "payment.succeeded")  # must not raise
    assert route.call_count == 2


async def test_send_merchant_webhook_gives_up_quietly_after_exhausting_retries(respx_mock, caplog):
    route = respx_mock.post("https://merchant.test/cb").mock(side_effect=httpx.ConnectError("boom"))
    payment = make_payment(callback_url="https://merchant.test/cb", status=PaymentStatus.FAILED)
    await send_merchant_webhook(payment, "payment.failed")  # must not raise
    assert route.call_count == 4  # 1 initial + 3 retries
    assert "boom" in caplog.text
