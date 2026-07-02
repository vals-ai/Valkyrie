"""Add task retry outcomes

Revision ID: 6f3c2d9a8b10
Revises: 9f1a69961211
Create Date: 2026-06-24 00:00:00.000000

"""

from datetime import UTC, datetime
from typing import Sequence, Union
from uuid import uuid4

import sqlalchemy as sa
from alembic import op

revision: str = "6f3c2d9a8b10"
down_revision: Union[str, Sequence[str], None] = "7c3d2a1f9b8e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "evaluationresult",
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
    )
    errorresult_table = op.create_table(
        "errorresult",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("task", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("error_message", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"]),
        sa.ForeignKeyConstraint(["task"], ["task.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    existing_error_rows = (
        op.get_bind()
        .execute(sa.text("SELECT id, org_id, finished_at, error_message FROM task WHERE error_message IS NOT NULL"))
        .mappings()
        .all()
    )
    current_time = datetime.now(UTC)
    if existing_error_rows:
        op.bulk_insert(
            errorresult_table,
            [
                {
                    "id": uuid4(),
                    "org_id": task_row["org_id"],
                    "task": task_row["id"],
                    "created_at": task_row["finished_at"] or current_time,
                    "error_message": task_row["error_message"],
                }
                for task_row in existing_error_rows
            ],
        )
    op.drop_column("task", "error_message")


def downgrade() -> None:
    op.add_column("task", sa.Column("error_message", sa.String(), nullable=True))
    op.execute(
        sa.text(
            """
            UPDATE task
            SET error_message = latest.error_message
            FROM (
                SELECT DISTINCT ON (task) task, error_message
                FROM errorresult
                ORDER BY task, created_at DESC
            ) AS latest
            WHERE task.id = latest.task
            """
        )
    )
    op.drop_table("errorresult")
    op.drop_column("evaluationresult", "created_at")
