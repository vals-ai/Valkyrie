"""Merge dev and generation elapsed migration heads.

Revision ID: 5e2a9f0c7d41
Revises: d9e0f1a2b3c4, 0a4d9c7e2b81
"""

from typing import Sequence, Union

revision: str = "5e2a9f0c7d41"
down_revision: Union[str, Sequence[str], None] = ("d9e0f1a2b3c4", "0a4d9c7e2b81")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
