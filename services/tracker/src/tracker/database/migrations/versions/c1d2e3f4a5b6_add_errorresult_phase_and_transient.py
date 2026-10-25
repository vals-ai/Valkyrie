"""Add failure phase and transient flag to error results.

Revision ID: c1d2e3f4a5b6
Revises: 9c0d760cb4ca
Create Date: 2026-10-15 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c1d2e3f4a5b6"
down_revision: str | Sequence[str] | None = "9c0d760cb4ca"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_ENUM_NAME = "failurephase"
_ENUM_VALUES = ("ADMISSION", "SANDBOX", "SETUP", "AGENT", "EVALUATION")


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '1s'")
    phase_enum = sa.Enum(*_ENUM_VALUES, name=_ENUM_NAME)
    phase_enum.create(op.get_bind(), checkfirst=True)
    op.add_column("errorresult", sa.Column("phase", phase_enum, nullable=True))
    op.add_column("errorresult", sa.Column("transient", sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column("errorresult", "transient")
    op.drop_column("errorresult", "phase")
    sa.Enum(name=_ENUM_NAME).drop(op.get_bind(), checkfirst=True)
