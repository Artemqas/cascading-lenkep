import asyncio

from app.providers.base import AttemptOutcome, OutcomeKind, ProviderAttemptRequest, WebhookResolution

_FINAL_CODES = {"insufficient_funds", "fraud_suspected"}


class FlakyAdapter:
    provider_id = "flaky"
    supports_status_check = True

    STATUS_RESOLUTIONS: dict[str, AttemptOutcome] = {}

    async def submit(self, request: ProviderAttemptRequest) -> AttemptOutcome:
        scenario = request.scenario
        if scenario == "timeout":
            raise asyncio.TimeoutError()
        status_code, body = self._raw_response(scenario)
        return self._normalize(status_code, body)

    def _raw_response(self, scenario: str) -> tuple[int, dict]:
        if scenario == "http_5xx":
            return 502, {}
        if scenario == "declined_via_http":
            return 403, {"code": "fraud_suspected"}
        if scenario == "declined_via_body":
            return 200, {"status": "declined", "code": "processing_error"}
        return 200, {}

    def _normalize(self, status_code: int, body: dict) -> AttemptOutcome:
        if status_code >= 500:
            return AttemptOutcome(kind=OutcomeKind.AMBIGUOUS)
        if status_code in (402, 403):
            return AttemptOutcome(kind=OutcomeKind.DECLINED_FINAL, decline_code=body["code"])
        if body.get("status") == "declined":
            code = body["code"]
            kind = OutcomeKind.DECLINED_FINAL if code in _FINAL_CODES else OutcomeKind.DECLINED_RETRYABLE
            return AttemptOutcome(kind=kind, decline_code=code)
        return AttemptOutcome(kind=OutcomeKind.SUCCESS)

    async def check_status(self, provider_reference: str) -> AttemptOutcome:
        if provider_reference not in self.STATUS_RESOLUTIONS:
            raise asyncio.TimeoutError()
        return self.STATUS_RESOLUTIONS[provider_reference]

    def parse_webhook(self, payload: dict) -> WebhookResolution:
        raise NotImplementedError("flaky resolves via status-check, not webhooks")
