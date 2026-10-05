"""Index processed events by time, for retention pruning.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-04 14:08:43.777286
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        op.f("ix_processed_events_processed_at"), "processed_events", ["processed_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_processed_events_processed_at"), table_name="processed_events")
