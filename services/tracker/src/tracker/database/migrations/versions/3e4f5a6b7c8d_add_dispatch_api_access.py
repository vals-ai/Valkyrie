"""Add dispatch-scoped executor API credentials and terminal receipts.

Revision ID: 3e4f5a6b7c8d
Revises: 2d3e4f5a6b7c
"""

import sqlalchemy as sa
from alembic import op

revision = "3e4f5a6b7c8d"
down_revision = "2d3e4f5a6b7c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "executordispatchaccess",
        sa.Column("dispatch_id", sa.Uuid(), nullable=False),
        sa.Column("token_digest", sa.String(), nullable=False),
        sa.Column("claimant_id", sa.Uuid(), nullable=True),
        sa.Column("terminal_operation", sa.String(), nullable=True),
        sa.Column("terminal_request_digest", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["dispatch_id"], ["executordispatch.id"]),
        sa.PrimaryKeyConstraint("dispatch_id"),
    )


def downgrade() -> None:
    op.drop_table("executordispatchaccess")
