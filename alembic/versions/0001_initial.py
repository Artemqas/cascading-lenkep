"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-11-11
"""

import sqlalchemy as sa

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

DIRECTION_VALUES = ("deposit", "withdraw")
CARD_TYPE_VALUES = ("visa", "mastercard", "other")
PAYMENT_STATUS_VALUES = ("pending", "processing", "succeeded", "failed", "conflict")
ATTEMPT_STATUS_VALUES = (
    "pending",
    "succeeded",
    "declined_final",
    "declined_retryable",
    "ambiguous",
    "timed_out",
)


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("direction", sa.Enum(*DIRECTION_VALUES, name="direction"), nullable=False),
        sa.Column("amount", sa.Integer(), nullable=False),
        sa.Column("currency", sa.String(), nullable=False),
        sa.Column("card_bin", sa.String(), nullable=True),
        sa.Column("card_type", sa.Enum(*CARD_TYPE_VALUES, name="card_type"), nullable=False),
        sa.Column(
            "status",
            sa.Enum(*PAYMENT_STATUS_VALUES, name="payment_status"),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("idempotency_key", sa.String(), nullable=False, unique=True),
        sa.Column("idempotency_payload_hash", sa.String(), nullable=False),
        sa.Column("callback_url", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "providers",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=True),
        sa.Column("supported_directions", sa.JSON(), nullable=False),
        sa.Column("supported_card_types", sa.JSON(), nullable=False),
        sa.Column("min_amount", sa.Integer(), nullable=False),
        sa.Column("max_amount", sa.Integer(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
    )

    op.create_table(
        "provider_attempts",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("provider_id", sa.String(), sa.ForeignKey("providers.id"), nullable=False),
        sa.Column(
            "status", sa.Enum(*ATTEMPT_STATUS_VALUES, name="attempt_status"), nullable=False
        ),
        sa.Column("decline_code", sa.String(), nullable=True),
        sa.Column("provider_reference", sa.String(), nullable=True),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "provider_stats",
        sa.Column("provider_id", sa.String(), sa.ForeignKey("providers.id"), primary_key=True),
        sa.Column(
            "direction",
            sa.Enum(*DIRECTION_VALUES, name="direction", create_type=False),
            primary_key=True,
        ),
        sa.Column(
            "card_type",
            sa.Enum(*CARD_TYPE_VALUES, name="card_type", create_type=False),
            primary_key=True,
        ),
        sa.Column("success_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("failure_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "reconciliation_flags",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("reason", sa.String(), nullable=False),
        sa.Column("attempt_ids", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("reconciliation_flags")
    op.drop_table("provider_stats")
    op.drop_table("provider_attempts")
    op.drop_table("providers")
    op.drop_table("payments")
