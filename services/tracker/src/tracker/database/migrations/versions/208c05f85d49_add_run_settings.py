"""Save sandbox provider secrets per organization and run defaults per user.

Revision ID: 208c05f85d49
Revises: 6a36c1a30b60
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "208c05f85d49"
down_revision: str | Sequence[str] | None = "6a36c1a30b60"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "org_provider_secret",
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("secret_name", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"]),
        sa.PrimaryKeyConstraint("org_id", "provider"),
    )
    op.create_table(
        "user_default_provider",
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"]),
        sa.PrimaryKeyConstraint("org_id", "user_id"),
    )
    op.create_table(
        "user_benchmark_service",
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("benchmark", sa.String(), nullable=False),
        sa.Column("url", sa.String(), nullable=False),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"]),
        sa.PrimaryKeyConstraint("org_id", "user_id", "benchmark"),
    )


def downgrade() -> None:
    op.drop_table("user_benchmark_service")
    op.drop_table("user_default_provider")
    op.drop_table("org_provider_secret")
