"""Add task attempts and factual failure records.

Revision ID: a3f4b5c6d7e8
Revises: f0a1b2c3d4e5
Create Date: 2026-08-13 00:00:00.000000
"""

from datetime import UTC, datetime
from typing import Sequence, Union
from uuid import uuid4

import sqlalchemy as sa
from alembic import op
from alembic.util import CommandError
from sqlalchemy.dialects import postgresql

revision: str = "a3f4b5c6d7e8"
down_revision: Union[str, Sequence[str], None] = "f0a1b2c3d4e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

task_attempt_outcome = postgresql.ENUM(
    "pending",
    "finished",
    "error",
    "stopped",
    name="taskattemptoutcome",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    task_attempt_outcome.create(bind, checkfirst=True)

    op.rename_table("errorresult", "failurerecord")
    op.alter_column(
        "failurerecord",
        "created_at",
        existing_type=sa.DateTime(),
        existing_nullable=False,
        new_column_name="occurred_at",
    )
    op.alter_column(
        "failurerecord",
        "error_message",
        existing_type=sa.String(),
        existing_nullable=False,
        new_column_name="message",
    )
    legacy_failure_count = bind.execute(sa.text("SELECT count(*) FROM failurerecord")).scalar_one()

    op.add_column("failurerecord", sa.Column("benchmark_id", sa.Uuid(), nullable=True))
    op.add_column("failurerecord", sa.Column("task_attempt_id", sa.Uuid(), nullable=True))
    op.add_column("failurerecord", sa.Column("dispatch_id", sa.Uuid(), nullable=True))
    op.add_column("failurerecord", sa.Column("producer", sa.String(), nullable=True))
    op.add_column("failurerecord", sa.Column("operation", sa.String(), nullable=True))
    op.add_column("failurerecord", sa.Column("error_type", sa.String(), nullable=True))
    op.add_column("failurerecord", sa.Column("cause_code", sa.String(), nullable=True))
    op.add_column("failurerecord", sa.Column("retry_scheduled", sa.Boolean(), nullable=True))
    op.add_column("failurerecord", sa.Column("safe_details", sa.JSON(), nullable=True))

    op.execute(
        sa.text(
            """
            UPDATE failurerecord AS failure
            SET benchmark_id = task.benchmark
            FROM task
            JOIN benchmark
              ON benchmark.id = task.benchmark
             AND benchmark.org_id = task.org_id
            WHERE failure.task = task.id
              AND failure.org_id = task.org_id
              AND benchmark.org_id = failure.org_id
            """
        )
    )
    if bind.execute(sa.text("SELECT count(*) FROM failurerecord")).scalar_one() != legacy_failure_count:
        raise CommandError("Legacy failure record count changed while deriving benchmark_id")
    unmapped_failure_count = bind.execute(
        sa.text("SELECT count(*) FROM failurerecord WHERE benchmark_id IS NULL")
    ).scalar_one()
    if unmapped_failure_count:
        raise CommandError(f"Cannot derive benchmark_id for {unmapped_failure_count} legacy failure records")

    op.execute(sa.text("UPDATE failurerecord SET retry_scheduled = false"))
    op.alter_column("failurerecord", "benchmark_id", existing_type=sa.Uuid(), nullable=False)
    op.alter_column("failurerecord", "retry_scheduled", existing_type=sa.Boolean(), nullable=False)
    op.alter_column("failurerecord", "task", existing_type=sa.Uuid(), nullable=True)
    op.create_foreign_key(
        "fk_failurerecord_benchmark_id_benchmark",
        "failurerecord",
        "benchmark",
        ["benchmark_id"],
        ["id"],
    )
    op.create_check_constraint(
        "ck_failurerecord_task_attempt_requires_task",
        "failurerecord",
        "task_attempt_id IS NULL OR task IS NOT NULL",
    )
    op.create_index(
        "ix_failurerecord_org_benchmark_occurred_at",
        "failurerecord",
        ["org_id", "benchmark_id", "occurred_at"],
    )
    op.create_index(
        "ix_failurerecord_org_task_occurred_at",
        "failurerecord",
        ["org_id", "task", "occurred_at"],
    )

    op.create_table(
        "taskattempt",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("task", sa.Uuid(), nullable=False),
        sa.Column("dispatch_id", sa.Uuid(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("outcome", task_attempt_outcome, nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"], name="fk_taskattempt_org_id_org"),
        sa.ForeignKeyConstraint(["task"], ["task.id"], name="fk_taskattempt_task_task"),
        sa.ForeignKeyConstraint(
            ["dispatch_id"], ["executordispatch.id"], name="fk_taskattempt_dispatch_id_executordispatch"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_foreign_key(
        "fk_failurerecord_task_attempt_id_taskattempt",
        "failurerecord",
        "taskattempt",
        ["task_attempt_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_failurerecord_dispatch_id_executordispatch",
        "failurerecord",
        "executordispatch",
        ["dispatch_id"],
        ["id"],
    )
    op.add_column("task", sa.Column("active_attempt_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        "fk_task_active_attempt_id_taskattempt",
        "task",
        "taskattempt",
        ["active_attempt_id"],
        ["id"],
    )
    op.add_column("evaluationresult", sa.Column("task_attempt_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        "fk_evaluationresult_task_attempt_id_taskattempt",
        "evaluationresult",
        "taskattempt",
        ["task_attempt_id"],
        ["id"],
    )

    benchmark_error_rows = (
        bind.execute(
            sa.text("SELECT id, org_id, finished_at, error_message FROM benchmark WHERE error_message IS NOT NULL")
        )
        .mappings()
        .all()
    )
    if benchmark_error_rows:
        failure_record_table = sa.table(
            "failurerecord",
            sa.column("id", sa.Uuid()),
            sa.column("org_id", sa.Uuid()),
            sa.column("benchmark_id", sa.Uuid()),
            sa.column("task", sa.Uuid()),
            sa.column("occurred_at", sa.DateTime()),
            sa.column("message", sa.String()),
            sa.column("retry_scheduled", sa.Boolean()),
        )
        current_time = datetime.now(UTC)
        op.bulk_insert(
            failure_record_table,
            [
                {
                    "id": uuid4(),
                    "org_id": benchmark_row["org_id"],
                    "benchmark_id": benchmark_row["id"],
                    "task": None,
                    "occurred_at": benchmark_row["finished_at"] or current_time,
                    "message": benchmark_row["error_message"],
                    "retry_scheduled": False,
                }
                for benchmark_row in benchmark_error_rows
            ],
        )

    expected_failure_count = legacy_failure_count + len(benchmark_error_rows)
    migrated_failure_count = bind.execute(sa.text("SELECT count(*) FROM failurerecord")).scalar_one()
    if migrated_failure_count != expected_failure_count:
        raise CommandError(
            f"Expected {expected_failure_count} migrated failure records, found {migrated_failure_count}"
        )


def downgrade() -> None:
    raise CommandError(
        "Migration a3f4b5c6d7e8 is irreversible because task attempts and failure histories cannot be reconstructed"
    )
