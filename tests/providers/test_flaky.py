import asyncio

import pytest

from app.enums import CardType, Direction
from app.providers.base import AttemptOutcome, OutcomeKind, ProviderAttemptRequest
from app.providers.flaky import FlakyAdapter


def make_request(scenario: str | None = None) -> ProviderAttemptRequest:
    return ProviderAttemptRequest(
        attempt_id="attempt-1",
        direction=Direction.DEPOSIT,
        amount=1000,
        currency="USD",
        card_type=CardType.VISA,
        scenario=scenario,
    )


async def test_flaky_timeout_scenario_raises():
    adapter = FlakyAdapter()
    with pytest.raises(asyncio.TimeoutError):
        await adapter.submit(make_request(scenario="timeout"))


async def test_flaky_normalizes_both_conventions():
    adapter = FlakyAdapter()
    http_outcome = await adapter.submit(make_request(scenario="declined_via_http"))
    body_outcome = await adapter.submit(make_request(scenario="declined_via_body"))
    assert http_outcome.kind == OutcomeKind.DECLINED_FINAL
    assert body_outcome.kind == OutcomeKind.DECLINED_RETRYABLE


async def test_flaky_check_status_returns_seeded_outcome():
    adapter = FlakyAdapter()
    adapter.STATUS_RESOLUTIONS["ref-1"] = AttemptOutcome(kind=OutcomeKind.SUCCESS)
    outcome = await adapter.check_status("ref-1")
    assert outcome.kind == OutcomeKind.SUCCESS
