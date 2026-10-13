"""Record the Descope access keys created for each run.

Revision ID: c2e7039fa933
Revises: a12b4c6d8e90
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c2e7039fa933"
down_revision: str | Sequence[str] | None = "a12b4c6d8e90"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "run_access_key",
        sa.Column("key_id", sa.String(), nullable=False),
        sa.Column("benchmark_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["benchmark_id"], ["benchmark.id"]),
        sa.PrimaryKeyConstraint("key_id"),
    )


def downgrade() -> None:
    op.drop_table("run_access_key")
