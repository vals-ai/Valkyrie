"""Add evaluation result history index.

Revision ID: 1c2d3e4f5a6b
Revises: 6a7b8c9d0e1f
Create Date: 2026-09-20 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "1c2d3e4f5a6b"
down_revision: Union[str, Sequence[str], None] = "6a7b8c9d0e1f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_INDEX_NAME = "ix_evaluationresult_org_task_created_at_id"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.create_index(
            _INDEX_NAME,
            "evaluationresult",
            ["org_id", "task", sa.text("created_at DESC"), sa.text("id DESC")],
            unique=False,
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            _INDEX_NAME,
            table_name="evaluationresult",
            postgresql_concurrently=True,
        )
