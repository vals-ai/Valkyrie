"""Add DEFERRED task status

Revision ID: e1a2b3c4d5f6
Revises: 35f2d4a8c9b1
Create Date: 2026-05-11 13:00:00.000000

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e1a2b3c4d5f6"
down_revision: Union[str, Sequence[str], None] = "35f2d4a8c9b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("ALTER TYPE taskstatus ADD VALUE IF NOT EXISTS 'DEFERRED' AFTER 'ERROR'")


def downgrade() -> None:
    """Downgrade schema."""
    # PostgreSQL does not support removing values from an enum type
    pass
