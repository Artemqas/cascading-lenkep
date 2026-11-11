import pytest

from app.enums import CardType, Direction
from app.providers.base import OutcomeKind, ProviderAttemptRequest
from app.providers.fast_sync import FastSyncAdapter


def make_request(scenario: str | None = None) -> ProviderAttemptRequest:
    return ProviderAttemptRequest(
        attempt_id="attempt-1",
        direction=Direction.DEPOSIT,
        amount=1000,
        currency="USD",
        card_type=CardType.VISA,
        scenario=scenario,
    )


@pytest.mark.parametrize("scenario,kind,code", [
    ("success", OutcomeKind.SUCCESS, None),
    ("insufficient_funds", OutcomeKind.DECLINED_FINAL, "insufficient_funds"),
    ("fraud_suspected", OutcomeKind.DECLINED_FINAL, "fraud_suspected"),
    ("processing_error", OutcomeKind.DECLINED_RETRYABLE, "processing_error"),
])
async def test_fast_sync_normalizes_http_status_declines(scenario, kind, code):
    adapter = FastSyncAdapter()
    outcome = await adapter.submit(make_request(scenario=scenario))
    assert outcome.kind == kind
    assert outcome.decline_code == code
