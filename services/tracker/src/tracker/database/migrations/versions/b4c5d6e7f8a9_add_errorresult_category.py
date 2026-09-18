"""Add failure category to error results.

Revision ID: b4c5d6e7f8a9
Revises: 6a7b8c9d0e1f
Create Date: 2026-09-17 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "b4c5d6e7f8a9"
down_revision: Union[str, Sequence[str], None] = "6a7b8c9d0e1f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_ENUM_NAME = "failurecategory"
_ENUM_VALUES = ("INFRASTRUCTURE", "BENCHMARK_SERVICE", "AGENT", "CANCELLED", "UNKNOWN")


def upgrade() -> None:
    category_enum = sa.Enum(*_ENUM_VALUES, name=_ENUM_NAME)
    category_enum.create(op.get_bind(), checkfirst=True)
    op.add_column("errorresult", sa.Column("category", category_enum, nullable=True))


def downgrade() -> None:
    op.drop_column("errorresult", "category")
    sa.Enum(name=_ENUM_NAME).drop(op.get_bind(), checkfirst=True)
