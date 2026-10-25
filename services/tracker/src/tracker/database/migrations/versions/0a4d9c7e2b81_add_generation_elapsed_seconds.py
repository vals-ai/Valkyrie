"""Add measured generation elapsed time to task breakdown.

Revision ID: 0a4d9c7e2b81
Revises: 9e0f1a2b3c4d
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0a4d9c7e2b81"
down_revision: Union[str, Sequence[str], None] = "9e0f1a2b3c4d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("taskbreakdown", sa.Column("generation_elapsed_seconds", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("taskbreakdown", "generation_elapsed_seconds")
