from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, Sequence[str], None] = "8bac4a96dd80"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Remove the env key from contract inside the arguments JSON column."""
    op.execute(
        """
        UPDATE benchmark
        SET arguments = jsonb_set(
            arguments::jsonb,
            '{contract}',
            (arguments::jsonb -> 'contract') - 'env'
        )
        WHERE arguments::jsonb -> 'contract' ? 'env'
        """
    )


def downgrade() -> None:
    pass
