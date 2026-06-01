"""merge dev + valkyrie heads

Revision ID: 11f02aed31bc
Revises: 9eef5075bc60, e5f6a7b8c9d0
Create Date: 2026-06-01 00:00:00.000000

"""

from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "11f02aed31bc"
down_revision: Union[str, Sequence[str], None] = ("9eef5075bc60", "e5f6a7b8c9d0")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
