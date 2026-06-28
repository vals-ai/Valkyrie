"""Add BUILDING task status

Revision ID: 8bac4a96dd80
Revises: b7edc066bc8c
Create Date: 2026-02-20 23:10:29.134022

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8bac4a96dd80"
down_revision: Union[str, Sequence[str], None] = "b7edc066bc8c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute("ALTER TYPE taskstatus ADD VALUE IF NOT EXISTS 'BUILDING' AFTER 'PENDING'")


def downgrade() -> None:
    """Downgrade schema."""
    # PostgreSQL does not support removing values from an enum type
    pass
