"""Add executor release lifecycle and benchmark ownership.

Revision ID: c7d8e9f0a1b2
Revises: 6f3c2d9a8b10
Create Date: 2026-07-20 00:00:00.000000
"""

from datetime import UTC, datetime
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c7d8e9f0a1b2"
down_revision: Union[str, Sequence[str], None] = "6f3c2d9a8b10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RELEASE_STATUS_VALUES = ("CANDIDATE", "ACTIVE", "DRAINING", "RETIRED")
_RELEASE_STATUS_ENUM = "executorreleasestatus"


def upgrade() -> None:
    release_status = postgresql.ENUM(*_RELEASE_STATUS_VALUES, name=_RELEASE_STATUS_ENUM, create_type=False)
    release_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "executorrelease",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("artifact_uri", sa.String(), nullable=False),
        sa.Column("artifact_digest", sa.String(), nullable=False),
        sa.Column("protocol_version", sa.String(), nullable=False),
        sa.Column("status", release_status, nullable=False),
        sa.Column("readiness_verified", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("readiness_metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("activated_at", sa.DateTime(), nullable=True),
        sa.Column("draining_at", sa.DateTime(), nullable=True),
        sa.Column("retired_at", sa.DateTime(), nullable=True),
        sa.Column("artifact_retention_until", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "executoradmission",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("release_id", sa.String(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["release_id"], ["executorrelease.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.bulk_insert(
        sa.table(
            "executoradmission",
            sa.column("id", sa.Integer()),
            sa.column("release_id", sa.String()),
            sa.column("updated_at", sa.DateTime()),
        ),
        [{"id": 1, "release_id": None, "updated_at": datetime.now(UTC)}],
    )
    op.add_column("benchmark", sa.Column("executor_release_id", sa.String(), nullable=True))
    op.add_column("benchmark", sa.Column("executor_artifact_uri", sa.String(), nullable=True))
    op.add_column("benchmark", sa.Column("executor_artifact_digest", sa.String(), nullable=True))
    op.add_column("benchmark", sa.Column("executor_protocol_version", sa.String(), nullable=True))
    op.create_check_constraint(
        "benchmark_executor_ownership_complete",
        "benchmark",
        "(executor_release_id IS NULL AND executor_artifact_uri IS NULL AND executor_artifact_digest IS NULL "
        "AND executor_protocol_version IS NULL) OR "
        "(executor_release_id IS NOT NULL AND executor_artifact_uri IS NOT NULL "
        "AND executor_artifact_digest IS NOT NULL AND executor_protocol_version IS NOT NULL)",
    )
    op.create_index("ix_benchmark_executor_release_id", "benchmark", ["executor_release_id"], unique=False)
    op.create_foreign_key(
        "fk_benchmark_executor_release_id",
        "benchmark",
        "executorrelease",
        ["executor_release_id"],
        ["id"],
    )


def downgrade() -> None:
    op.drop_constraint("benchmark_executor_ownership_complete", "benchmark", type_="check")
    op.drop_constraint("fk_benchmark_executor_release_id", "benchmark", type_="foreignkey")
    op.drop_index("ix_benchmark_executor_release_id", table_name="benchmark")
    op.drop_column("benchmark", "executor_protocol_version")
    op.drop_column("benchmark", "executor_artifact_digest")
    op.drop_column("benchmark", "executor_artifact_uri")
    op.drop_column("benchmark", "executor_release_id")
    op.drop_table("executoradmission")
    op.drop_table("executorrelease")
    sa.Enum(name=_RELEASE_STATUS_ENUM).drop(op.get_bind(), checkfirst=True)
