"""Add per-dispatch executor release ownership.

Revision ID: d8e9f0a1b2c3
Revises: c7d8e9f0a1b2
Create Date: 2026-07-20 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d8e9f0a1b2c3"
down_revision: Union[str, Sequence[str], None] = "c7d8e9f0a1b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DISPATCH_KIND_VALUES = ("START", "RETRY", "RESUME")
_DISPATCH_KIND_ENUM = "executordispatchkind"
_DISPATCH_STATUS_VALUES = ("QUEUED", "RUNNING", "FINISHED", "FAILED")
_DISPATCH_STATUS_ENUM = "executordispatchstatus"


def upgrade() -> None:
    dispatch_kind = postgresql.ENUM(*_DISPATCH_KIND_VALUES, name=_DISPATCH_KIND_ENUM, create_type=False)
    dispatch_status = postgresql.ENUM(*_DISPATCH_STATUS_VALUES, name=_DISPATCH_STATUS_ENUM, create_type=False)
    dispatch_kind.create(op.get_bind(), checkfirst=True)
    dispatch_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "executordispatch",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("benchmark_id", sa.Uuid(), nullable=False),
        sa.Column("kind", dispatch_kind, nullable=False),
        sa.Column("status", dispatch_status, nullable=False),
        sa.Column("executor_release_id", sa.String(), nullable=False),
        sa.Column("executor_artifact_uri", sa.String(), nullable=False),
        sa.Column("executor_artifact_digest", sa.String(), nullable=False),
        sa.Column("executor_protocol_version", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["benchmark_id"], ["benchmark.id"]),
        sa.ForeignKeyConstraint(["executor_release_id"], ["executorrelease.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_executordispatch_release_status",
        "executordispatch",
        ["executor_release_id", "status"],
        unique=False,
    )
    op.create_index(
        "ix_executordispatch_benchmark_kind",
        "executordispatch",
        ["benchmark_id", "kind"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_executordispatch_benchmark_kind", table_name="executordispatch")
    op.drop_index("ix_executordispatch_release_status", table_name="executordispatch")
    op.drop_table("executordispatch")
    sa.Enum(name=_DISPATCH_STATUS_ENUM).drop(op.get_bind(), checkfirst=True)
    sa.Enum(name=_DISPATCH_KIND_ENUM).drop(op.get_bind(), checkfirst=True)
