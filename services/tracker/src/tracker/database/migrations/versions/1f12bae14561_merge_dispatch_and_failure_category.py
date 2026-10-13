"""Merge deployed dispatch and failure-category migration heads.

Revision ID: 1f12bae14561
Revises: 3e4f5a6b7c8d, b4c5d6e7f8a9
"""

from collections.abc import Sequence

revision: str = "1f12bae14561"
down_revision: str | Sequence[str] | None = ("3e4f5a6b7c8d", "b4c5d6e7f8a9")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
