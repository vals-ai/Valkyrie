"""Add persistent executor sandbox-creation reservations.

Revision ID: 6b7c8d9e0f1a
Revises: 5a6b7c8d9e0f
"""

import sqlalchemy as sa
from alembic import op

revision = "6b7c8d9e0f1a"
down_revision = "5a6b7c8d9e0f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "executorpoolreservation",
        sa.Column("pool_id", sa.String(), nullable=False),
        sa.Column("reservation_id", sa.Uuid(), nullable=False),
        sa.Column("dispatch_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["dispatch_id"], ["executordispatch.id"]),
        sa.ForeignKeyConstraint(["task_id"], ["task.id"]),
        sa.PrimaryKeyConstraint("pool_id"),
    )


def downgrade() -> None:
    op.drop_table("executorpoolreservation")
