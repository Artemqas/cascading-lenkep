import uuid
from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import JSON, Enum as SAEnum, ForeignKey, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base
from app.enums import AttemptStatus, CardType, Direction, PaymentStatus


def _enum_values(enum_cls: type[Enum]) -> list[str]:
    return [member.value for member in enum_cls]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    direction: Mapped[Direction] = mapped_column(SAEnum(Direction, values_callable=_enum_values))
    amount: Mapped[int]
    currency: Mapped[str]
    card_bin: Mapped[str | None]
    card_type: Mapped[CardType] = mapped_column(SAEnum(CardType, values_callable=_enum_values))
    status: Mapped[PaymentStatus] = mapped_column(
        SAEnum(PaymentStatus, values_callable=_enum_values), default=PaymentStatus.PENDING
    )
    idempotency_key: Mapped[str] = mapped_column(unique=True)
    idempotency_payload_hash: Mapped[str]
    callback_url: Mapped[str]
    # provider_id -> mock scenario string, for deterministic demo/test behavior; unused in production.
    provider_scenarios: Mapped[dict[str, str] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(default=_utcnow, onupdate=_utcnow)
    # Optimistic lock: the cascade loop and a provider webhook arriving on a
    # separate session both read-then-write this row. Without a version check,
    # whichever commits last silently wins instead of detecting the race.
    version: Mapped[int] = mapped_column(default=1, nullable=False)

    __mapper_args__ = {"version_id_col": version}


class Provider(Base):
    __tablename__ = "providers"

    id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    priority: Mapped[int | None]
    supported_directions: Mapped[list[str]] = mapped_column(JSON)
    supported_card_types: Mapped[list[str]] = mapped_column(JSON)
    min_amount: Mapped[int]
    max_amount: Mapped[int]
    enabled: Mapped[bool] = mapped_column(default=True)


class ProviderAttempt(Base):
    __tablename__ = "provider_attempts"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    payment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("payments.id"))
    provider_id: Mapped[str] = mapped_column(ForeignKey("providers.id"))
    status: Mapped[AttemptStatus] = mapped_column(SAEnum(AttemptStatus, values_callable=_enum_values))
    decline_code: Mapped[str | None]
    provider_reference: Mapped[str | None]
    requested_at: Mapped[datetime] = mapped_column(default=_utcnow)
    resolved_at: Mapped[datetime | None]


class ProviderStats(Base):
    __tablename__ = "provider_stats"

    provider_id: Mapped[str] = mapped_column(ForeignKey("providers.id"), primary_key=True)
    direction: Mapped[Direction] = mapped_column(
        SAEnum(Direction, values_callable=_enum_values), primary_key=True
    )
    card_type: Mapped[CardType] = mapped_column(
        SAEnum(CardType, values_callable=_enum_values), primary_key=True
    )
    success_count: Mapped[int] = mapped_column(default=0)
    failure_count: Mapped[int] = mapped_column(default=0)
    updated_at: Mapped[datetime] = mapped_column(default=_utcnow, onupdate=_utcnow)


class ReconciliationFlag(Base):
    __tablename__ = "reconciliation_flags"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    payment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("payments.id"))
    reason: Mapped[str]
    attempt_ids: Mapped[list[str]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(default=_utcnow)
    resolved_at: Mapped[datetime | None]
