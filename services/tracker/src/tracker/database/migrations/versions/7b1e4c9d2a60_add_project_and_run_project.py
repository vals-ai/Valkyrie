"""Record projects and link each run to the project it spends for.

Revision ID: 7b1e4c9d2a60
Revises: b7e4c1a9d2f0
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "7b1e4c9d2a60"
down_revision: str | Sequence[str] | None = "b7e4c1a9d2f0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ENUM_NAME = "spendtype"
_ENUM_VALUES = ("DEVELOPMENT", "INTERNAL", "CUSTOMER")


def upgrade() -> None:
    op.create_table(
        "project",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("handle", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("spend_type", sa.Enum(*_ENUM_VALUES, name=_ENUM_NAME), nullable=False),
        sa.Column("subcategory", sa.String(), nullable=True),
        sa.Column("customer", sa.String(), nullable=True),
        sa.Column("extra_tags", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by_email", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("org_id", "handle", name="unique_project_handle_per_org"),
    )
    op.create_table(
        "run_project",
        sa.Column("id", sa.Integer(), sa.Identity(), nullable=False),
        sa.Column("benchmark_id", sa.Uuid(), nullable=False),
        sa.Column("project_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by_email", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["benchmark_id"], ["benchmark.id"]),
        sa.ForeignKeyConstraint(["project_id"], ["project.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_run_project_benchmark_id"), "run_project", ["benchmark_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_run_project_benchmark_id"), table_name="run_project")
    op.drop_table("run_project")
    op.drop_table("project")
    sa.Enum(name=_ENUM_NAME).drop(op.get_bind(), checkfirst=True)
