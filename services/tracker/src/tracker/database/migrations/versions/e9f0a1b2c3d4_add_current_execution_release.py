"""Add benchmark current execution release ownership.

Revision ID: e9f0a1b2c3d4
Revises: d8e9f0a1b2c3
Create Date: 2026-07-21 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from alembic.util import CommandError

revision: str = "e9f0a1b2c3d4"
down_revision: Union[str, Sequence[str], None] = "d8e9f0a1b2c3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "benchmark",
        sa.Column("current_execution_release_id", sa.String(), nullable=True),
    )
    op.create_index(
        "ix_benchmark_current_execution_release_id",
        "benchmark",
        ["current_execution_release_id"],
        unique=False,
    )
    op.create_foreign_key(
        "fk_benchmark_current_execution_release_id_executorrelease",
        "benchmark",
        "executorrelease",
        ["current_execution_release_id"],
        ["id"],
    )


def downgrade() -> None:
    raise CommandError(
        "Migration e9f0a1b2c3d4 is irreversible because downgrading would destroy "
        "current execution release ownership; retain the additive schema and roll forward"
    )
