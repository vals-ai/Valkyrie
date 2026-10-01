"""Merge dispatch payload and credited generation deadline migration heads.

Revision ID: 9e0f1a2b3c4d
Revises: 3e4f5a6b7c8d, 8d9e0f1a2b3c
"""

from typing import Sequence, Union

revision: str = "9e0f1a2b3c4d"
down_revision: Union[str, Sequence[str], None] = ("3e4f5a6b7c8d", "8d9e0f1a2b3c")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
