import pytest

from app.enums import CardType, Direction
from app.providers.base import OutcomeKind, ProviderAttemptRequest
from app.providers.webhook_async import WebhookAsyncAdapter


def make_request(scenario: str | None = None) -> ProviderAttemptRequest:
    return ProviderAttemptRequest(
        attempt_id="attempt-1",
        direction=Direction.DEPOSIT,
        amount=1000,
        currency="USD",
        card_type=CardType.VISA,
        scenario=scenario,
    )


async def test_webhook_async_submit_is_always_ambiguous():
    adapter = WebhookAsyncAdapter()
    outcome = await adapter.submit(make_request(scenario="success"))
    assert outcome.kind == OutcomeKind.AMBIGUOUS


@pytest.mark.parametrize("payload,kind,code", [
    ({"attempt_id": "a1", "status": "approved"}, OutcomeKind.SUCCESS, None),
    ({"attempt_id": "a1", "status": "declined", "code": "fraud_suspected"}, OutcomeKind.DECLINED_FINAL, "fraud_suspected"),
    ({"attempt_id": "a1", "status": "declined", "code": "processing_error"}, OutcomeKind.DECLINED_RETRYABLE, "processing_error"),
])
def test_webhook_async_parse_webhook_normalizes_body_coded_result(payload, kind, code):
    adapter = WebhookAsyncAdapter()
    resolution = adapter.parse_webhook(payload)
    assert resolution.attempt_id == "a1"
    assert resolution.outcome.kind == kind
    assert resolution.outcome.decline_code == code
