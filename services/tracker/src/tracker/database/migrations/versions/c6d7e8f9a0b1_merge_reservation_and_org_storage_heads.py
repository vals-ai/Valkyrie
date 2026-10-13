"""Merge sandbox reservation and org storage migration heads.

Revision ID: c6d7e8f9a0b1
Revises: 4f5a6b7c8d9e, a7b8c9d0e1f2
"""

revision: str = "c6d7e8f9a0b1"
down_revision: tuple[str, str] = ("4f5a6b7c8d9e", "a7b8c9d0e1f2")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join migration histories without schema changes."""
    pass


def downgrade() -> None:
    """Split migration histories without schema changes."""
    pass
