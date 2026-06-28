"""drop benchmark.run_by_email (consolidate on started_by_email)

Backfills historical run_by_email values onto started_by_email, then drops the
redundant column — the run starter (from x-api-key) is the single source of truth.

Revision ID: b2c3d4e5f6a7
Revises: 9f1a69961211
Create Date: 2026-06-24 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
import sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, Sequence[str], None] = "9f1a69961211"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "UPDATE benchmark SET started_by_email = run_by_email "
        "WHERE started_by_email IS NULL AND run_by_email IS NOT NULL"
    )
    op.drop_column("benchmark", "run_by_email")


def downgrade() -> None:
    op.add_column(
        "benchmark",
        sa.Column("run_by_email", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    )
    op.execute("UPDATE benchmark SET run_by_email = started_by_email")
