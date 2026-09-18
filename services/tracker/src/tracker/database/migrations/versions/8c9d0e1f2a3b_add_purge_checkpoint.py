"""Retain minimal purge proof after the run rows are removed."""

import sqlalchemy as sa
from alembic import op
from alembic.util import CommandError

revision = "8c9d0e1f2a3b"
down_revision = "7b8c9d0e1f2a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("runlifecycle", sa.Column("checkpoint_json", sa.Text(), nullable=True))


def downgrade() -> None:
    raise CommandError("Purge checkpoints must be retained; roll forward")
