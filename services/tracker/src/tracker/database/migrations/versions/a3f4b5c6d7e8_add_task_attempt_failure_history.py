"""Add task attempts and failure records.

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

failure_category = postgresql.ENUM(
    "valkyrie",
    "daytona",
    "harness",
    "model",
    "model_gateway",
    "unknown",
    name="failurecategory",
    create_type=False,
)
failure_classification_state = postgresql.ENUM(
    "classified",
    "unclassified",
    "details_unavailable",
    "legacy_unclassified",
    name="failureclassificationstate",
    create_type=False,
)
failure_terminal_effect = postgresql.ENUM(
    "recovered",
    "secondary",
    "terminal",
    name="failureterminaleffect",
    create_type=False,
)
task_attempt_admission_reason = postgresql.ENUM(
    "initial",
    "manual_retry",
    "resume",
    "automatic_retry",
    "rollout_claim",
    name="taskattemptadmissionreason",
    create_type=False,
)
task_attempt_outcome = postgresql.ENUM(
    "pending",
    "finished",
    "error",
    "stopped",
    "superseded",
    name="taskattemptoutcome",
    create_type=False,
)


def upgrade() -> None:
    bind = op.get_bind()
    for enum_type in (
        failure_category,
        failure_classification_state,
        failure_terminal_effect,
        task_attempt_admission_reason,
        task_attempt_outcome,
    ):
        enum_type.create(bind, checkfirst=True)

    op.rename_table("errorresult", "failurerecord")
    legacy_failure_count = bind.execute(sa.text("SELECT count(*) FROM failurerecord")).scalar_one()

    op.add_column("failurerecord", sa.Column("schema_version", sa.Integer(), nullable=True))
    op.add_column("failurerecord", sa.Column("benchmark_id", sa.Uuid(), nullable=True))
    op.add_column("failurerecord", sa.Column("task_attempt_id", sa.Uuid(), nullable=True))
    op.add_column("failurerecord", sa.Column("dispatch_id", sa.Uuid(), nullable=True))
    op.add_column("failurerecord", sa.Column("retry_sequence", sa.Integer(), nullable=True))
    op.add_column("failurerecord", sa.Column("category", failure_category, nullable=True))
    op.add_column("failurerecord", sa.Column("producer", sa.String(), nullable=True))
    op.add_column("failurerecord", sa.Column("operation", sa.String(), nullable=True))
    op.add_column("failurerecord", sa.Column("error_type", sa.String(), nullable=True))
    op.add_column("failurerecord", sa.Column("classification_state", failure_classification_state, nullable=True))
    op.add_column("failurerecord", sa.Column("cause_code", sa.String(), nullable=True))
    op.add_column("failurerecord", sa.Column("terminal_effect", failure_terminal_effect, nullable=True))
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

    op.execute(
        sa.text(
            """
            UPDATE failurerecord
            SET schema_version = 1,
                category = 'unknown'::failurecategory,
                classification_state = 'legacy_unclassified'::failureclassificationstate,
                terminal_effect = 'terminal'::failureterminaleffect
            """
        )
    )
    op.alter_column("failurerecord", "schema_version", existing_type=sa.Integer(), nullable=False)
    op.alter_column("failurerecord", "benchmark_id", existing_type=sa.Uuid(), nullable=False)
    op.alter_column("failurerecord", "category", existing_type=failure_category, nullable=False)
    op.alter_column(
        "failurerecord",
        "classification_state",
        existing_type=failure_classification_state,
        nullable=False,
    )
    op.alter_column("failurerecord", "terminal_effect", existing_type=failure_terminal_effect, nullable=False)
    op.alter_column("failurerecord", "task", existing_type=sa.Uuid(), nullable=True)
    op.create_foreign_key(
        "fk_failurerecord_benchmark_id_benchmark",
        "failurerecord",
        "benchmark",
        ["benchmark_id"],
        ["id"],
    )
    op.create_check_constraint(
        "ck_failurerecord_schema_version_positive",
        "failurerecord",
        "schema_version > 0",
    )
    op.create_check_constraint(
        "ck_failurerecord_retry_sequence_nonnegative",
        "failurerecord",
        "retry_sequence IS NULL OR retry_sequence >= 0",
    )
    op.create_check_constraint(
        "ck_failurerecord_task_attempt_requires_task",
        "failurerecord",
        "task_attempt_id IS NULL OR task IS NOT NULL",
    )
    op.create_check_constraint(
        "ck_failurerecord_classification_cause",
        "failurerecord",
        "(classification_state = 'classified' AND cause_code IS NOT NULL) "
        "OR (classification_state != 'classified' AND cause_code IS NULL)",
    )
    op.create_index(
        "ix_failurerecord_org_benchmark_created_at",
        "failurerecord",
        ["org_id", "benchmark_id", "created_at"],
    )
    op.create_index(
        "ix_failurerecord_org_task_created_at",
        "failurerecord",
        ["org_id", "task", "created_at"],
    )

    op.create_table(
        "taskattempt",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("task", sa.Uuid(), nullable=False),
        sa.Column("dispatch_id", sa.Uuid(), nullable=True),
        sa.Column("previous_attempt_id", sa.Uuid(), nullable=True),
        sa.Column("superseded_by_attempt_id", sa.Uuid(), nullable=True),
        sa.Column("admission_reason", task_attempt_admission_reason, nullable=False),
        sa.Column("reason_failure_id", sa.Uuid(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("outcome", task_attempt_outcome, nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"], name="fk_taskattempt_org_id_org"),
        sa.ForeignKeyConstraint(["task"], ["task.id"], name="fk_taskattempt_task_task"),
        sa.ForeignKeyConstraint(
            ["dispatch_id"], ["executordispatch.id"], name="fk_taskattempt_dispatch_id_executordispatch"
        ),
        sa.ForeignKeyConstraint(
            ["previous_attempt_id"], ["taskattempt.id"], name="fk_taskattempt_previous_attempt_id_taskattempt"
        ),
        sa.ForeignKeyConstraint(
            ["superseded_by_attempt_id"],
            ["taskattempt.id"],
            name="fk_taskattempt_superseded_by_attempt_id_taskattempt",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_taskattempt_org_task_started_at", "taskattempt", ["org_id", "task", "started_at"])
    op.create_index("ix_taskattempt_dispatch_id", "taskattempt", ["dispatch_id"])

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
    op.create_foreign_key(
        "fk_taskattempt_reason_failure_id_failurerecord",
        "taskattempt",
        "failurerecord",
        ["reason_failure_id"],
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
    op.create_index("ix_task_active_attempt_id", "task", ["active_attempt_id"])

    op.add_column("evaluationresult", sa.Column("task_attempt_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        "fk_evaluationresult_task_attempt_id_taskattempt",
        "evaluationresult",
        "taskattempt",
        ["task_attempt_id"],
        ["id"],
    )
    op.create_index("ix_evaluationresult_task_attempt_id", "evaluationresult", ["task_attempt_id"])

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
            sa.column("schema_version", sa.Integer()),
            sa.column("org_id", sa.Uuid()),
            sa.column("benchmark_id", sa.Uuid()),
            sa.column("task", sa.Uuid()),
            sa.column("created_at", sa.DateTime()),
            sa.column("category", failure_category),
            sa.column("error_message", sa.String()),
            sa.column("classification_state", failure_classification_state),
            sa.column("terminal_effect", failure_terminal_effect),
        )
        current_time = datetime.now(UTC)
        op.bulk_insert(
            failure_record_table,
            [
                {
                    "id": uuid4(),
                    "schema_version": 1,
                    "org_id": benchmark_row["org_id"],
                    "benchmark_id": benchmark_row["id"],
                    "task": None,
                    "created_at": benchmark_row["finished_at"] or current_time,
                    "category": "unknown",
                    "error_message": benchmark_row["error_message"],
                    "classification_state": "legacy_unclassified",
                    "terminal_effect": "terminal",
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
