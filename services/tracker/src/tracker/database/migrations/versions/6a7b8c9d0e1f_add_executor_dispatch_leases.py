"""Add durable executor dispatch lease state.

Revision ID: 6a7b8c9d0e1f
Revises: 50c3051116fa
Create Date: 2026-08-20 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from alembic.util import CommandError
from sqlalchemy.dialects import postgresql

revision: str = "6a7b8c9d0e1f"
down_revision: Union[str, Sequence[str], None] = "50c3051116fa"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("executordispatch", sa.Column("assigned_task_ids", postgresql.JSON(), nullable=True))
    op.add_column("executordispatch", sa.Column("claim_deadline_at", sa.DateTime(), nullable=True))
    op.add_column("executordispatch", sa.Column("heartbeat_at", sa.DateTime(), nullable=True))
    op.add_column("executordispatch", sa.Column("lease_expires_at", sa.DateTime(), nullable=True))
    op.add_column("executordispatch", sa.Column("failure_reason", sa.String(), nullable=True))
    op.create_index(
        "ix_executordispatch_status_lease_expires",
        "executordispatch",
        ["status", "lease_expires_at"],
        unique=False,
    )

    # Preserve the existing timestamp ownership boundary for dispatches that
    # were created before the lease fields existed.
    op.execute(
        sa.text(
            """
            UPDATE executordispatch AS dispatch
            SET assigned_task_ids = COALESCE(
                    (
                        SELECT json_agg(task.task_id ORDER BY task.task_id)
                        FROM task
                        WHERE task.benchmark = dispatch.benchmark_id
                          AND task.started_at <= dispatch.created_at
                    ),
                    '[]'::json
                ),
                claim_deadline_at = dispatch.created_at + INTERVAL '120 seconds',
                heartbeat_at = CASE
                    WHEN dispatch.status = 'RUNNING' THEN dispatch.started_at
                    ELSE NULL
                END,
                lease_expires_at = CASE
                    WHEN dispatch.status = 'RUNNING' AND dispatch.started_at IS NOT NULL
                    THEN dispatch.started_at + INTERVAL '300 seconds'
                    ELSE NULL
                END
            """
        )
    )


def downgrade() -> None:
    raise CommandError(
        "Migration 6a7b8c9d0e1f is irreversible because removing dispatch lease state "
        "would reopen worker-loss recovery races; retain the additive schema and roll forward"
    )
