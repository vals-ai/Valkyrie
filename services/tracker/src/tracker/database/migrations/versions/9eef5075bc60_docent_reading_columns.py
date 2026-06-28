"""docent_reading_columns

Revision ID: 9eef5075bc60
Revises: a92e64afa650
Create Date: 2026-05-18 16:00:37.964833

"""

import sqlalchemy as sa
from alembic import op
from typing import Sequence, Union


# revision identifiers, used by Alembic.
revision: str = "9eef5075bc60"
down_revision: Union[str, Sequence[str], None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

DOCENT_READING_STATUS_VALUES = ("IDLE", "RUNNING", "ERROR", "DONE")


def upgrade() -> None:
    """Upgrade schema."""
    docent_status_enum = sa.Enum(*DOCENT_READING_STATUS_VALUES, name="docentreadingstatus")
    docent_status_enum.create(op.get_bind(), checkfirst=True)

    op.add_column(
        "benchmark",
        sa.Column(
            "docent_reading_status",
            docent_status_enum,
            nullable=False,
            server_default="IDLE",
        ),
    )
    op.add_column(
        "benchmark",
        sa.Column("docent_reading_url", sa.String(), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("benchmark", "docent_reading_url")
    op.drop_column("benchmark", "docent_reading_status")
    sa.Enum(name="docentreadingstatus").drop(op.get_bind(), checkfirst=True)
