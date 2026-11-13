"""add payments.version (optimistic lock) and payment_status.review_required

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-13
"""

import sqlalchemy as sa

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TYPE payment_status ADD VALUE IF NOT EXISTS 'review_required'")
    op.add_column(
        "payments",
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
    )


def downgrade() -> None:
    op.drop_column("payments", "version")
    # Postgres has no "DROP VALUE" for enum types; removing 'review_required'
    # would require recreating payment_status and re-pointing the column,
    # which isn't worth it for a downgrade path.
