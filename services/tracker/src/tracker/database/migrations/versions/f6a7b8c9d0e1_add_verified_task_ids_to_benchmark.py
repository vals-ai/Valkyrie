"""Add persisted verified task IDs to benchmark runs."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f6a7b8c9d0e1"
down_revision: str | Sequence[str] | None = "e5f6a7b8c9d0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("benchmark", sa.Column("verified_task_ids", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("benchmark", "verified_task_ids")
