"""add priority to tasks

Revision ID: 6b37a1f33f02
Revises: d8eb91e1692a
Create Date: 2026-08-15 15:30:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6b37a1f33f02"
down_revision = "d8eb91e1692a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add priority column to tasks."""
    op.add_column(
        "tasks",
        sa.Column(
            "priority",
            sa.Enum(
                "LOW",
                "NORMAL",
                "HIGH",
                "CRITICAL",
                name="task_priority",
                native_enum=False,
                create_constraint=False,
                length=50,
            ),
            nullable=False,
            server_default=sa.text("'NORMAL'"),
        ),
    )


def downgrade() -> None:
    """Remove priority column from tasks."""
    op.drop_column("tasks", "priority")
