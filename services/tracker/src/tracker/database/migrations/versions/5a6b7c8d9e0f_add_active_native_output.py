"""Add the internal active native output attachment.

Revision ID: 5a6b7c8d9e0f
Revises: 5e2a9f0c7d41
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "5a6b7c8d9e0f"
down_revision: str | Sequence[str] | None = "5e2a9f0c7d41"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("task", sa.Column("active_native_output", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("task", "active_native_output")
