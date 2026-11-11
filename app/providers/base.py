from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from app.enums import CardType, Direction


class OutcomeKind(str, Enum):
    SUCCESS = "success"
    DECLINED_FINAL = "declined_final"
    DECLINED_RETRYABLE = "declined_retryable"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True)
class AttemptOutcome:
    kind: OutcomeKind
    decline_code: str | None = None
    provider_reference: str | None = None


@dataclass(frozen=True)
class ProviderAttemptRequest:
    attempt_id: str
    direction: Direction
    amount: int
    currency: str
    card_type: CardType
    scenario: str | None = None


@dataclass(frozen=True)
class WebhookResolution:
    attempt_id: str
    outcome: AttemptOutcome


class ProviderAdapter(Protocol):
    provider_id: str
    supports_status_check: bool

    async def submit(self, request: ProviderAttemptRequest) -> AttemptOutcome: ...

    async def check_status(self, provider_reference: str) -> AttemptOutcome: ...

    def parse_webhook(self, payload: dict) -> WebhookResolution: ...
