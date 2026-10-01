from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


revision: str = "b3c4d5e6f7a8"
down_revision: Union[str, Sequence[str], None] = "2d3e4f5a6b7c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_column("benchmark", "webhook_intervals")
    op.drop_column("benchmark", "webhook_secret_name")


def downgrade() -> None:
    op.add_column("benchmark", sa.Column("webhook_secret_name", sqlmodel.sql.sqltypes.AutoString(), nullable=True))
    op.add_column("benchmark", sa.Column("webhook_intervals", sa.JSON(), nullable=True))
