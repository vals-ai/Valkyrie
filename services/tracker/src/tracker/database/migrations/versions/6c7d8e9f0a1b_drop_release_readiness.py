"""Remove release readiness fields after the image migration.

Revision ID: 6c7d8e9f0a1b
Revises: 5b6c7d8e9f0a
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "6c7d8e9f0a1b"
down_revision: Union[str, Sequence[str], None] = "5b6c7d8e9f0a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("executorrelease", "readiness_metadata")
    op.drop_column("executorrelease", "readiness_verified")


def downgrade() -> None:
    op.add_column(
        "executorrelease",
        sa.Column("readiness_verified", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.add_column(
        "executorrelease",
        sa.Column("readiness_metadata", sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
    )
