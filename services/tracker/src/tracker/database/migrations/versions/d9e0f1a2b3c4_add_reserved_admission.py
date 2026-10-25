"""Add domain-qualified admission candidates and per-attempt reservation accounting.

Revision ID: d9e0f1a2b3c4
Revises: c1d2e3f4a5b6
"""

import sqlalchemy as sa
from alembic import op

revision: str = "d9e0f1a2b3c4"
down_revision: str = "c1d2e3f4a5b6"
branch_labels = None
depends_on = None

_RESERVATIONS = "sandboxbuildreservation"
_CANDIDATES = "sandboxadmissioncandidate"
_CONSTRAINT = "sandbox_build_reservation_resources_nonnegative"
_RESERVATION_PK = "pk_sandbox_build_reservation"
_RELEASED_INDEX = "ix_sandboxbuildreservation_pool_released_at"


def upgrade() -> None:
    # Pre-domain reservations carry no quota domain; their builds are already past admission.
    op.execute(sa.text(f"DELETE FROM {_RESERVATIONS}"))
    op.add_column(_RESERVATIONS, sa.Column("released_at", sa.DateTime(), nullable=True))
    op.create_index(_RELEASED_INDEX, _RESERVATIONS, ["pool_id", "released_at"], unique=False)

    # Released rows outlive the attempt, so a later attempt of the same task needs its own row.
    op.drop_constraint(_RESERVATION_PK, _RESERVATIONS, type_="primary")
    op.create_primary_key(_RESERVATION_PK, _RESERVATIONS, ["task_row_id", "attempt_started_at"])
    op.add_column(_RESERVATIONS, sa.Column("target_id", sa.String(), nullable=False))
    op.add_column(_RESERVATIONS, sa.Column("sandbox_class", sa.String(), nullable=False))
    op.add_column(_RESERVATIONS, sa.Column("requested_gpu", sa.Float(), nullable=False))
    for name in ("requested_vcpu", "requested_memory", "requested_disk"):
        op.alter_column(_RESERVATIONS, name, existing_type=sa.Integer(), type_=sa.Float(), existing_nullable=False)
    op.drop_constraint(_CONSTRAINT, _RESERVATIONS, type_="check")
    op.create_check_constraint(
        _CONSTRAINT,
        _RESERVATIONS,
        "requested_vcpu >= 0 AND requested_memory >= 0 AND requested_disk >= 0 AND requested_gpu >= 0",
    )
    op.create_table(
        _CANDIDATES,
        sa.Column("task_row_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_started_at", sa.DateTime(), nullable=False),
        sa.Column("executor_dispatch_id", sa.Uuid(), nullable=False),
        sa.Column("pool_id", sa.String(), nullable=False),
        sa.Column("target_id", sa.String(), nullable=False),
        sa.Column("sandbox_class", sa.String(), nullable=False),
        sa.Column("requested_vcpu", sa.Float(), nullable=False),
        sa.Column("requested_memory", sa.Float(), nullable=False),
        sa.Column("requested_disk", sa.Float(), nullable=False),
        sa.Column("requested_gpu", sa.Float(), nullable=False),
        sa.Column("admission_ready", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("task_row_id", "attempt_started_at"),
        sa.ForeignKeyConstraint(["task_row_id"], ["task.id"]),
        sa.ForeignKeyConstraint(["executor_dispatch_id"], ["executordispatch.id"]),
    )
    op.create_index("ix_sandboxadmissioncandidate_pool", _CANDIDATES, ["pool_id"])


def downgrade() -> None:
    op.drop_index("ix_sandboxadmissioncandidate_pool", table_name=_CANDIDATES)
    op.drop_table(_CANDIDATES)
    op.drop_constraint(_CONSTRAINT, _RESERVATIONS, type_="check")
    op.create_check_constraint(
        _CONSTRAINT,
        _RESERVATIONS,
        "requested_vcpu >= 0 AND requested_memory >= 0 AND requested_disk >= 0",
    )
    for name in ("requested_vcpu", "requested_memory", "requested_disk"):
        op.alter_column(
            _RESERVATIONS,
            name,
            existing_type=sa.Float(),
            type_=sa.Integer(),
            existing_nullable=False,
            postgresql_using=f"{name}::integer",
        )
    op.drop_column(_RESERVATIONS, "requested_gpu")
    op.drop_column(_RESERVATIONS, "sandbox_class")
    op.drop_column(_RESERVATIONS, "target_id")
    op.execute(sa.text(f"DELETE FROM {_RESERVATIONS} WHERE released_at IS NOT NULL"))
    op.drop_constraint(_RESERVATION_PK, _RESERVATIONS, type_="primary")
    op.create_primary_key(_RESERVATION_PK, _RESERVATIONS, ["task_row_id"])
    op.drop_index(_RELEASED_INDEX, table_name=_RESERVATIONS)
    op.drop_column(_RESERVATIONS, "released_at")
