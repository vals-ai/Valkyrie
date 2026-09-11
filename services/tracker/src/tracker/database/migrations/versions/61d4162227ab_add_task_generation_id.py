"""Keep explicit fresh generations separate from automatic recovery."""

import sqlalchemy as sa
from alembic import op

revision = "61d4162227ab"
down_revision = "50c3051116fa"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("task", sa.Column("generation_id", sa.Uuid(), nullable=True))


def downgrade() -> None:
    op.drop_column("task", "generation_id")
