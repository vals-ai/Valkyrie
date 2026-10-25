"""Record each run's sandboxes and the provider cost billed for them.

Revision ID: b7e4c1a9d2f0
Revises: 5a6b7c8d9e0f
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b7e4c1a9d2f0"
down_revision: str | Sequence[str] | None = "5a6b7c8d9e0f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sandbox",
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("sandbox_id", sa.String(), nullable=False),
        sa.Column("benchmark_id", sa.Uuid(), nullable=False),
        sa.ForeignKeyConstraint(["benchmark_id"], ["benchmark.id"]),
        sa.PrimaryKeyConstraint("provider", "sandbox_id"),
    )
    op.create_index("ix_sandbox_benchmark_id", "sandbox", ["benchmark_id"])
    op.create_table(
        "run_cost",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("benchmark_id", sa.Uuid(), nullable=False),
        sa.Column("cost_type", sa.String(), nullable=False),
        sa.Column("source_id", sa.String(), nullable=True),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("amount_usd", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(["benchmark_id"], ["benchmark.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_run_cost_benchmark_id", "run_cost", ["benchmark_id"])
    op.create_index(
        "uq_run_cost_sandbox_provider_source_id",
        "run_cost",
        ["provider", "source_id"],
        unique=True,
        postgresql_where=sa.text("cost_type = 'sandbox'"),
    )


def downgrade() -> None:
    op.drop_index("uq_run_cost_sandbox_provider_source_id", table_name="run_cost")
    op.drop_index("ix_run_cost_benchmark_id", table_name="run_cost")
    op.drop_table("run_cost")
    op.drop_index("ix_sandbox_benchmark_id", table_name="sandbox")
    op.drop_table("sandbox")
