"""Drop the benchmark flag that separated runs started with caller AWS keys.

Every AWS run now uses the deployment's credentials, and Tracker decides run
storage access from the organization's bucket alone.

Revision ID: c4d5e6f7a8b9
Revises: a7b8c9d0e1f2
Create Date: 2026-10-06 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c4d5e6f7a8b9"
down_revision: Union[str, Sequence[str], None] = "a7b8c9d0e1f2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("benchmark", "aws_managed")


def downgrade() -> None:
    """Restore the flag, marking every AWS run as using the deployment's credentials."""
    op.add_column(
        "benchmark",
        sa.Column("aws_managed", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.execute(
        sa.text("UPDATE benchmark SET aws_managed = true WHERE COALESCE(arguments->>'environment', 'aws') = 'aws'")
    )
