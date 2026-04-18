"""Remove artifacts field from contract

Revision ID: f8c7b6a5d4e3
Revises: 2093e351e8bf
Create Date: 2026-03-08 15:30:00.000000

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f8c7b6a5d4e3"
down_revision: Union[str, Sequence[str], None] = "2093e351e8bf"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Remove the artifacts field from contract in the arguments JSON column."""
    op.execute(
        """
        UPDATE benchmark
        SET arguments = jsonb_set(
            arguments::jsonb,
            '{contract}',
            (arguments::jsonb -> 'contract') - 'artifacts'
        )
        WHERE arguments::jsonb -> 'contract' ? 'artifacts'
        """
    )


def downgrade() -> None:
    pass
