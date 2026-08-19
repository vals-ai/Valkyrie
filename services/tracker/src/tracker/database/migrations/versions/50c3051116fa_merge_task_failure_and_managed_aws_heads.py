"""merge task failure and managed AWS heads

Revision ID: 50c3051116fa
Revises: a3f4b5c6d7e8, e5f6a7b8c9d0
Create Date: 2026-08-19 00:15:58.535240

"""

from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "50c3051116fa"
down_revision: Union[str, Sequence[str], None] = ("a3f4b5c6d7e8", "e5f6a7b8c9d0")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
