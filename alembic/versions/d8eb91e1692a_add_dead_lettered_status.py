"""add dead_lettered task status

Revision ID: d8eb91e1692a
Revises: 8d2c7f8a7f21
Create Date: 2026-08-15 14:00:00.000000
"""

from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "d8eb91e1692a"
down_revision = "8d2c7f8a7f21"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Document the DEAD_LETTERED status addition.

    The status column is VARCHAR(50) -- no DDL change is required for a new
    string value. This migration records the change in version history and
    provides a safe downgrade path.
    """
    # No DDL required: VARCHAR(50) accepts the new 'DEAD_LETTERED' value.
    pass


def downgrade() -> None:
    """Neutralise DEAD_LETTERED rows before the status value is removed from code.

    Any task in DEAD_LETTERED is semantically equivalent to a final FAILED state,
    so reverting to FAILED is safe and preserves audit trail continuity.
    """
    op.execute(
        "UPDATE tasks SET status = 'FAILED' WHERE status = 'DEAD_LETTERED'"
    )
