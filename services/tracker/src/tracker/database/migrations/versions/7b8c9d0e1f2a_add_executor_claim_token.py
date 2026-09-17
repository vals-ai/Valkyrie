"""Add claimant identity for transient local secret handoff."""

import sqlalchemy as sa
from alembic import op

revision = "7b8c9d0e1f2a"
down_revision = "6a7b8c9d0e1f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("executordispatch", sa.Column("claim_token", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("executordispatch", "claim_token")
