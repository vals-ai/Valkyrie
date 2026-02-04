"""Updated contract and agent output

Revision ID: 26b342c24bd4
Revises: 2a83996162a5
Create Date: 2026-01-29 12:22:43.282031

"""

from typing import Sequence, Union

import sqlalchemy as sa
import sqlmodel  # noqa: F401 # type: ignore
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "26b342c24bd4"
down_revision: Union[str, Sequence[str], None] = "2a83996162a5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    connection = op.get_bind()

    # Convert empty strings to empty JSON objects BEFORE changing the column type
    connection.execute(
        text("""
        UPDATE evaluationresult
        SET agent_output = '{}'
        WHERE agent_output = '' OR agent_output IS NULL
    """)
    )

    # Use raw SQL for PostgreSQL to specify the USING clause for type cast
    connection.execute(
        text("ALTER TABLE evaluationresult ALTER COLUMN agent_output TYPE JSON USING agent_output::json")
    )


def downgrade() -> None:
    pass
