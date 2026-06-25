"""add run label

Revision ID: 7c3d2a1f9b8e
Revises: b2c3d4e5f6a7
Create Date: 2026-06-24 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = "7c3d2a1f9b8e"
down_revision: Union[str, Sequence[str], None] = "b2c3d4e5f6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("benchmark", sa.Column("label", sqlmodel.sql.sqltypes.AutoString(), nullable=True))
    op.create_index(op.f("ix_benchmark_label"), "benchmark", ["label"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_benchmark_label"), table_name="benchmark")
    op.drop_column("benchmark", "label")
