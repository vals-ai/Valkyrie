"""Persist a generation identity for explicit from-scratch retries."""

import sqlalchemy as sa
from alembic import op

revision = "3e4f5a6b7c8d"
down_revision = "2d3e4f5a6b7c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("task", sa.Column("generation_id", sa.Uuid(), nullable=True))


def downgrade() -> None:
    op.drop_column("task", "generation_id")
