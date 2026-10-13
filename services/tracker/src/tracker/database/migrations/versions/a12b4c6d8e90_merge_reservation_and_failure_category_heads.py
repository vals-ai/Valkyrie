"""Merge sandbox reservation and failure-category migration heads.

Revision ID: a12b4c6d8e90
Revises: d5e6f7a8b9c0, fd632373e7ac
"""

revision: str = "a12b4c6d8e90"
down_revision: tuple[str, str] = ("d5e6f7a8b9c0", "fd632373e7ac")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join migration histories without schema changes."""
    pass


def downgrade() -> None:
    """Split migration histories without schema changes."""
    pass
