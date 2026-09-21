"""Add task listing index.

Revision ID: 2d3e4f5a6b7c
Revises: 1c2d3e4f5a6b
Create Date: 2026-09-21 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "2d3e4f5a6b7c"
down_revision: Union[str, Sequence[str], None] = "1c2d3e4f5a6b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_INDEX_NAME = "ix_task_benchmark_org_started_at"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.create_index(
            _INDEX_NAME,
            "task",
            ["benchmark", "org_id", sa.text("started_at DESC")],
            unique=False,
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            _INDEX_NAME,
            table_name="task",
            postgresql_concurrently=True,
        )
