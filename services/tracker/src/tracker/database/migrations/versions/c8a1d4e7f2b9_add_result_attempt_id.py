"""Add persisted task attempt evidence."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c8a1d4e7f2b9"
down_revision: str | Sequence[str] | None = "e5f6a7b8c9d0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("evaluationresult", sa.Column("attempt_id", sa.String(length=32), nullable=True))
    op.add_column("errorresult", sa.Column("attempt_id", sa.String(length=32), nullable=True))
    op.create_table(
        "taskattempt",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("task", sa.Uuid(), nullable=False),
        sa.Column("attempt_id", sa.String(length=32), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("sandbox_provider", sa.String(), nullable=False),
        sa.Column("sandbox_instance_id", sa.String(length=300), nullable=True),
        sa.Column("sandbox_snapshot", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"]),
        sa.ForeignKeyConstraint(["task"], ["task.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task", "attempt_id", name="unique_task_attempt"),
    )
    op.create_index(
        "ix_taskattempt_org_task_started_at",
        "taskattempt",
        ["org_id", "task", sa.literal_column("started_at DESC")],
        unique=False,
    )
    op.create_index(
        "ix_evaluationresult_org_task_created_at_id",
        "evaluationresult",
        ["org_id", "task", sa.literal_column("created_at DESC"), sa.literal_column("id DESC")],
        unique=False,
    )
    op.create_index(
        "ix_errorresult_org_task_created_at_id",
        "errorresult",
        ["org_id", "task", sa.literal_column("created_at DESC"), sa.literal_column("id DESC")],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_errorresult_org_task_created_at_id", table_name="errorresult")
    op.drop_index("ix_evaluationresult_org_task_created_at_id", table_name="evaluationresult")
    op.drop_index("ix_taskattempt_org_task_started_at", table_name="taskattempt")
    op.drop_table("taskattempt")
    op.drop_column("errorresult", "attempt_id")
    op.drop_column("evaluationresult", "attempt_id")
