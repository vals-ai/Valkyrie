"""Merge failure-category and benchmark aws_managed migration heads.

Revision ID: fd632373e7ac
Revises: 1f12bae14561, c4d5e6f7a8b9
"""

from collections.abc import Sequence

revision: str = "fd632373e7ac"
down_revision: str | Sequence[str] | None = ("1f12bae14561", "c4d5e6f7a8b9")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
