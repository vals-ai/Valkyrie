"""benchmark composite index on org_id started_at id

Revision ID: 7dc84861cc16
Revises: 2c902b6c56ba
Create Date: 2026-05-21 09:28:32.005130

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = '7dc84861cc16'
down_revision: Union[str, Sequence[str], None] = '2c902b6c56ba'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(
        "ix_benchmark_org_started_at_id",
        "benchmark",
        ["org_id", sa.text("started_at DESC"), "id"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_benchmark_org_started_at_id", table_name="benchmark")
