"""Add executor run-finalization receipts.

Revision ID: 5a6b7c8d9e0f
Revises: 4f5a6b7c8d9e
"""

import sqlalchemy as sa
from alembic import op

revision = "5a6b7c8d9e0f"
down_revision = "4f5a6b7c8d9e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "executorrunreceipt",
        sa.Column("dispatch_id", sa.Uuid(), nullable=False),
        sa.Column("command_id", sa.Uuid(), nullable=False),
        sa.Column("request_digest", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("final_evaluation_id", sa.Uuid(), nullable=True),
        sa.ForeignKeyConstraint(["dispatch_id"], ["executordispatch.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("dispatch_id", "command_id"),
    )


def downgrade() -> None:
    op.drop_table("executorrunreceipt")
