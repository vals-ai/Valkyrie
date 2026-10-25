"""Remove obsolete claim and heartbeat timestamps; retain the local process lease.

Revision ID: 5b6c7d8e9f0a
Revises: 4a5b6c7d8e9f
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "5b6c7d8e9f0a"
down_revision: Union[str, Sequence[str], None] = "4a5b6c7d8e9f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("executordispatch", "claim_deadline_at")
    op.drop_column("executordispatch", "heartbeat_at")


def downgrade() -> None:
    op.add_column("executordispatch", sa.Column("claim_deadline_at", sa.DateTime(), nullable=True))
    op.add_column("executordispatch", sa.Column("heartbeat_at", sa.DateTime(), nullable=True))
