"""Add durable sandbox build reservations."""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "3e4f5a6b7c8d"
down_revision: Union[str, Sequence[str], None] = "2d3e4f5a6b7c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TABLE_NAME = "sandboxbuildreservation"
_TRIGGER_NAME = "trg_guard_sandbox_build_reservation"
_FUNCTION_NAME = "guard_sandbox_build_reservation"


def upgrade() -> None:
    op.create_table(
        _TABLE_NAME,
        sa.Column("build_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("task_row_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_started_at", sa.DateTime(), nullable=False),
        sa.Column("executor_dispatch_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("pool_id", sa.String(), nullable=False),
        sa.Column("requested_vcpu", sa.Integer(), nullable=False),
        sa.Column("requested_memory", sa.Integer(), nullable=False),
        sa.Column("requested_disk", sa.Integer(), nullable=False),
        sa.Column("requested_gpu", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.CheckConstraint(
            "requested_vcpu >= 0 AND requested_memory >= 0 AND requested_disk >= 0 AND requested_gpu >= 0",
            name="sandbox_build_reservation_resources_nonnegative",
        ),
        sa.ForeignKeyConstraint(
            ["task_row_id"],
            ["task.id"],
            name="fk_sandbox_build_reservation_task",
        ),
        sa.ForeignKeyConstraint(
            ["executor_dispatch_id"],
            ["executordispatch.id"],
            name="fk_sandbox_build_reservation_dispatch",
        ),
        sa.PrimaryKeyConstraint("build_id", name="pk_sandbox_build_reservation"),
    )
    op.create_index(
        "uq_sandboxbuildreservation_task",
        _TABLE_NAME,
        ["task_row_id"],
        unique=True,
    )
    op.create_index(
        "ix_sandboxbuildreservation_pool",
        _TABLE_NAME,
        ["pool_id"],
        unique=False,
    )

    op.execute(
        sa.text(
            f"""
            CREATE FUNCTION {_FUNCTION_NAME}()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            BEGIN
                IF OLD.status = 'BUILDING' AND NEW.status = 'PENDING' AND EXISTS (
                    SELECT 1
                    FROM {_TABLE_NAME} AS reservation
                    WHERE reservation.task_row_id = OLD.id
                )
                THEN
                    RETURN OLD;
                END IF;
                -- Task state cannot confirm provider cleanup. Only the build
                -- owner releases its reservation after promotion or deletion.
                RETURN NEW;
            END;
            $$
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER {_TRIGGER_NAME}
            BEFORE UPDATE ON task
            FOR EACH ROW
            EXECUTE FUNCTION {_FUNCTION_NAME}()
            """
        )
    )


def downgrade() -> None:
    op.execute(sa.text(f"DROP TRIGGER IF EXISTS {_TRIGGER_NAME} ON task"))
    op.execute(sa.text(f"DROP FUNCTION IF EXISTS {_FUNCTION_NAME}()"))
    op.drop_index("ix_sandboxbuildreservation_pool", table_name=_TABLE_NAME)
    op.drop_index("uq_sandboxbuildreservation_task", table_name=_TABLE_NAME)
    op.drop_table(_TABLE_NAME)
