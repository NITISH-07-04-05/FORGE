"""add retry fields to tasks

Revision ID: 8d2c7f8a7f21
Revises: 47ad31af0f9c
Create Date: 2026-08-15 13:15:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8d2c7f8a7f21"
down_revision = "47ad31af0f9c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add durable retry state to tasks."""
    op.add_column(
        "tasks",
        sa.Column(
            "max_retries",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        "tasks",
        sa.Column(
            "retry_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        "tasks",
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "tasks",
        sa.Column("retry_enqueued_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    """Remove the retry columns."""
    op.drop_column("tasks", "next_retry_at")
    op.drop_column("tasks", "retry_count")
    op.drop_column("tasks", "max_retries")
    op.drop_column("tasks", "retry_enqueued_at")
