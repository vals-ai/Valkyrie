"""Add immutable AWS resource bindings to benchmark runs.

Revision ID: a6b7c8d9e0f1
Revises: 50c3051116fa
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a6b7c8d9e0f1"
down_revision: str | Sequence[str] | None = "50c3051116fa"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("benchmark", sa.Column("run_aws_resources", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("benchmark", "run_aws_resources")
