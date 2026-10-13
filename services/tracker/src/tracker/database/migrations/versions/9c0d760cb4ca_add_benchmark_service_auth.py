"""Let a saved benchmark service carry an organization secret to send as a header.

Revision ID: 9c0d760cb4ca
Revises: 208c05f85d49
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "9c0d760cb4ca"
down_revision: str | Sequence[str] | None = "208c05f85d49"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("user_benchmark_service", sa.Column("auth_header_name", sa.String(), nullable=True))
    op.add_column("user_benchmark_service", sa.Column("auth_secret_name", sa.String(), nullable=True))


def downgrade() -> None:
    op.drop_column("user_benchmark_service", "auth_secret_name")
    op.drop_column("user_benchmark_service", "auth_header_name")
