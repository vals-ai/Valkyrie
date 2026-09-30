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


def upgrade() -> None:
    op.create_table(
        _TABLE_NAME,
        sa.Column("task_row_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_started_at", sa.DateTime(), nullable=False),
        sa.Column("pool_id", sa.String(), nullable=False),
        sa.Column("requested_vcpu", sa.Integer(), nullable=False),
        sa.Column("requested_memory", sa.Integer(), nullable=False),
        sa.Column("requested_disk", sa.Integer(), nullable=False),
        sa.Column("requested_gpu", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "requested_vcpu >= 0 AND requested_memory >= 0 AND requested_disk >= 0 AND requested_gpu >= 0",
            name="sandbox_build_reservation_resources_nonnegative",
        ),
        sa.ForeignKeyConstraint(
            ["task_row_id"],
            ["task.id"],
            name="fk_sandbox_build_reservation_task",
        ),
        sa.PrimaryKeyConstraint("task_row_id", name="pk_sandbox_build_reservation"),
    )
    op.create_index(
        "ix_sandboxbuildreservation_pool",
        _TABLE_NAME,
        ["pool_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_sandboxbuildreservation_pool", table_name=_TABLE_NAME)
    op.drop_table(_TABLE_NAME)
