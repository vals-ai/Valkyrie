"""add run attribution columns

Revision ID: d4e5f6a7b8c9
Revises: 35f2d4a8c9b1
Create Date: 2026-05-05 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, Sequence[str], None] = "35f2d4a8c9b1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("benchmark", sa.Column("started_by_id", sa.String(), nullable=True))
    op.add_column("benchmark", sa.Column("started_by_email", sa.String(), nullable=True))
    op.create_index(op.f("ix_benchmark_started_by_email"), "benchmark", ["started_by_email"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_benchmark_started_by_email"), table_name="benchmark")
    op.drop_column("benchmark", "started_by_email")
    op.drop_column("benchmark", "started_by_id")
