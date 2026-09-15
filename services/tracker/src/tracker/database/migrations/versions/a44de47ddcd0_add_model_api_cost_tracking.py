"""Add per-task model API cost tracking.

Revision ID: a44de47ddcd0
Revises: 6a7b8c9d0e1f
Create Date: 2026-09-15 12:38:41.344187
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "a44de47ddcd0"
down_revision: Union[str, Sequence[str], None] = "6a7b8c9d0e1f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("task", sa.Column("model_api_cost_usd", sa.Numeric(), nullable=True))


def downgrade() -> None:
    op.drop_column("task", "model_api_cost_usd")
