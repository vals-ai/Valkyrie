"""Store the immutable customer log history reference on the run."""

import sqlalchemy as sa
from alembic import op
from alembic.util import CommandError

revision = "9d0e1f2a3b4c"
down_revision = "8c9d0e1f2a3b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("benchmark", sa.Column("log_history", sa.JSON(), nullable=True))


def downgrade() -> None:
    raise CommandError("Customer history references must be retained; roll forward")
