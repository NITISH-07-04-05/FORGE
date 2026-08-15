"""add_scheduled_at_and_scheduled_status

Revision ID: 4a95324ffebc
Revises: 6b37a1f33f02
Create Date: 2026-08-15 22:01:32.317067
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "4a95324ffebc"
down_revision = "6b37a1f33f02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Apply the schema changes for this revision."""
    op.add_column("tasks", sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    """Revert the schema changes for this revision."""
    op.drop_column("tasks", "scheduled_at")
