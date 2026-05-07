"""Add evaluation resume state

Revision ID: 35f2d4a8c9b1
Revises: a9d2e1c8b3f4
Create Date: 2026-05-04 13:55:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "35f2d4a8c9b1"
down_revision: Union[str, Sequence[str], None] = "a9d2e1c8b3f4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("task", sa.Column("eval_resume_state", postgresql.JSON(astext_type=sa.Text()), nullable=True))
    op.alter_column("evaluationresult", "instance_id", existing_type=sa.VARCHAR(), nullable=True)


def downgrade() -> None:
    op.alter_column("evaluationresult", "instance_id", existing_type=sa.VARCHAR(), nullable=False)
    op.drop_column("task", "eval_resume_state")
