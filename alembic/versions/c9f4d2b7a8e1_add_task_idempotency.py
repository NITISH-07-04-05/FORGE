"""add task idempotency

Revision ID: c9f4d2b7a8e1
Revises: 4a95324ffebc
Create Date: 2026-08-22 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c9f4d2b7a8e1"
down_revision = "4a95324ffebc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add task submission idempotency metadata and enforce uniqueness."""
    op.add_column("tasks", sa.Column("idempotency_key", sa.String(length=255), nullable=True))
    op.add_column("tasks", sa.Column("request_fingerprint", sa.String(length=64), nullable=True))
    op.create_unique_constraint(
        "uq_tasks_idempotency_key",
        "tasks",
        ["idempotency_key"],
    )


def downgrade() -> None:
    """Remove task submission idempotency metadata."""
    op.drop_constraint("uq_tasks_idempotency_key", "tasks", type_="unique")
    op.drop_column("tasks", "request_fingerprint")
    op.drop_column("tasks", "idempotency_key")
