"""Add task-attempt ownership and executor command receipts.

Revision ID: 4f5a6b7c8d9e
Revises: 3e4f5a6b7c8d
"""

import sqlalchemy as sa
from alembic import op

revision = "4f5a6b7c8d9e"
down_revision = "3e4f5a6b7c8d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "executortaskattempt",
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("dispatch_id", sa.Uuid(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], ["task.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dispatch_id"], ["executordispatch.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("task_id"),
    )
    op.create_table(
        "executortaskreceipt",
        sa.Column("dispatch_id", sa.Uuid(), nullable=False),
        sa.Column("command_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("request_digest", sa.String(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["dispatch_id"], ["executordispatch.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["task_id"], ["task.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("dispatch_id", "command_id"),
    )


def downgrade() -> None:
    op.drop_table("executortaskreceipt")
    op.drop_table("executortaskattempt")
