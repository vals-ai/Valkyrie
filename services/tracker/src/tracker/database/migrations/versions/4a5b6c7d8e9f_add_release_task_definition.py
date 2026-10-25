"""Pin image releases to their registered runner task definition.

Revision ID: 4a5b6c7d8e9f
Revises: 7b1e4c9d2a60
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "4a5b6c7d8e9f"
down_revision: Union[str, Sequence[str], None] = "7b1e4c9d2a60"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("executorrelease", sa.Column("task_definition_arn", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("executorrelease", "task_definition_arn")
