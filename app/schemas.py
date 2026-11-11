from datetime import datetime

from pydantic import BaseModel

from app.enums import AttemptStatus, CardType, Direction, PaymentStatus


class PaymentCreateRequest(BaseModel):
    direction: Direction
    amount: int
    currency: str
    card_bin: str | None = None
    card_type: CardType
    callback_url: str
    # provider_id -> mock scenario string, for deterministic demo/test behavior; unused in production.
    provider_scenarios: dict[str, str] | None = None


class PaymentResponse(BaseModel):
    id: str
    direction: Direction
    amount: int
    currency: str
    status: PaymentStatus
    created_at: datetime


class AttemptResponse(BaseModel):
    id: str
    provider_id: str
    status: AttemptStatus
    decline_code: str | None
    requested_at: datetime
    resolved_at: datetime | None


class PaymentDetailResponse(PaymentResponse):
    attempts: list[AttemptResponse]
