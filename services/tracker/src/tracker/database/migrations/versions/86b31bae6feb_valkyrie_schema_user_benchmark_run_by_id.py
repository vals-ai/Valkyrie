"""valkyrie schema: drop user + org_config, add benchmark.run_by_email + composite index

Revision ID: 86b31bae6feb
Revises: 9eef5075bc60
Create Date: 2026-06-01 22:29:42.319130

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


revision: str = "86b31bae6feb"
down_revision: Union[str, Sequence[str], None] = "9eef5075bc60"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("benchmark", sa.Column("run_by_email", sqlmodel.sql.sqltypes.AutoString(), nullable=True))
    op.create_index(
        "ix_benchmark_org_started_at_id",
        "benchmark",
        ["org_id", sa.literal_column("started_at DESC"), "id"],
        unique=False,
    )

    op.drop_index("ix_benchmark_run_by_id", table_name="benchmark")
    op.drop_constraint("benchmark_run_by_id_fkey", "benchmark", type_="foreignkey")
    op.drop_column("benchmark", "run_by_id")

    op.drop_table("org_config")

    op.drop_index("ix_user_descope_user_id", table_name="user")
    op.drop_index("ix_user_email", table_name="user")
    op.drop_index("ix_user_org_id", table_name="user")
    op.drop_table("user")


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_benchmark_org_started_at_id", table_name="benchmark")
    op.drop_column("benchmark", "run_by_email")
