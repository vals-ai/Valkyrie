"""Add task outcome history indexes

Revision ID: c4e8a1b2d3f5
Revises: 6f3c2d9a8b10
Create Date: 2026-07-27 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c4e8a1b2d3f5"
down_revision: Union[str, Sequence[str], None] = "6f3c2d9a8b10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for table in ("evaluationresult", "errorresult"):
        op.create_index(
            f"ix_{table}_org_task_created_at_id",
            table,
            ["org_id", "task", sa.text("created_at DESC"), sa.text("id DESC")],
        )


def downgrade() -> None:
    for table in ("errorresult", "evaluationresult"):
        op.drop_index(f"ix_{table}_org_task_created_at_id", table_name=table)
