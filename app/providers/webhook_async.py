from app.providers.base import AttemptOutcome, OutcomeKind, ProviderAttemptRequest, WebhookResolution

_FINAL_CODES = {"insufficient_funds", "fraud_suspected"}


class WebhookAsyncAdapter:
    provider_id = "webhook_async"
    supports_status_check = False

    async def submit(self, request: ProviderAttemptRequest) -> AttemptOutcome:
        status_code, body = 200, {"status": "pending"}
        return self._normalize_submit(status_code, body)

    def _normalize_submit(self, status_code: int, body: dict) -> AttemptOutcome:
        return AttemptOutcome(kind=OutcomeKind.AMBIGUOUS)

    def parse_webhook(self, payload: dict) -> WebhookResolution:
        attempt_id = payload["attempt_id"]
        status = payload["status"]
        if status == "approved":
            outcome = AttemptOutcome(kind=OutcomeKind.SUCCESS)
        else:
            code = payload["code"]
            kind = OutcomeKind.DECLINED_FINAL if code in _FINAL_CODES else OutcomeKind.DECLINED_RETRYABLE
            outcome = AttemptOutcome(kind=kind, decline_code=code)
        return WebhookResolution(attempt_id=attempt_id, outcome=outcome)

    async def check_status(self, provider_reference: str) -> AttemptOutcome:
        raise NotImplementedError("webhook_async does not support status checks")
