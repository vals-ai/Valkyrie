"""Merge sandbox reservation and managed run migration heads.

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9, c6d7e8f9a0b1
"""

revision: str = "d5e6f7a8b9c0"
down_revision: tuple[str, str] = ("c4d5e6f7a8b9", "c6d7e8f9a0b1")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join migration histories without schema changes."""
    pass


def downgrade() -> None:
    """Split migration histories without schema changes."""
    pass
