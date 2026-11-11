import random

from app.providers.base import AttemptOutcome, OutcomeKind, ProviderAttemptRequest

_SCENARIOS: dict[str, tuple[int, dict]] = {
    "success": (200, {}),
    "insufficient_funds": (402, {"code": "insufficient_funds"}),
    "fraud_suspected": (403, {"code": "fraud_suspected"}),
    "processing_error": (503, {"code": "processing_error"}),
}


class FastSyncAdapter:
    provider_id = "fast_sync"
    supports_status_check = False

    async def submit(self, request: ProviderAttemptRequest) -> AttemptOutcome:
        scenario = request.scenario or self._random_scenario()
        status_code, body = self._raw_response(scenario)
        return self._normalize(status_code, body)

    def _random_scenario(self) -> str:
        if random.random() < 0.9:
            return "success"
        return random.choice(["insufficient_funds", "fraud_suspected", "processing_error"])

    def _raw_response(self, scenario: str) -> tuple[int, dict]:
        return _SCENARIOS[scenario]

    def _normalize(self, status_code: int, body: dict) -> AttemptOutcome:
        if status_code == 200:
            return AttemptOutcome(kind=OutcomeKind.SUCCESS)
        if status_code in (402, 403):
            return AttemptOutcome(kind=OutcomeKind.DECLINED_FINAL, decline_code=body["code"])
        return AttemptOutcome(kind=OutcomeKind.DECLINED_RETRYABLE, decline_code=body["code"])

    async def check_status(self, provider_reference: str) -> AttemptOutcome:
        raise NotImplementedError("fast_sync does not support status checks")

    def parse_webhook(self, payload: dict):
        raise NotImplementedError("fast_sync never sends webhooks")
