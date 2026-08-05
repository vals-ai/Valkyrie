"""Add executor deployment maintenance ownership.

Revision ID: f0a1b2c3d4e5
Revises: e9f0a1b2c3d4
Create Date: 2026-07-27 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from alembic.util import CommandError

revision: str = "f0a1b2c3d4e5"
down_revision: Union[str, Sequence[str], None] = "e9f0a1b2c3d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "executoradmission",
        sa.Column("maintenance_target_sha", sa.String(), nullable=True),
    )


def downgrade() -> None:
    raise CommandError(
        "Migration f0a1b2c3d4e5 is irreversible because removing the maintenance fence "
        "could reopen executor admission during a deployment; retain the additive schema"
    )
