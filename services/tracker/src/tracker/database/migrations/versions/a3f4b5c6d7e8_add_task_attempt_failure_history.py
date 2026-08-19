"""Add factual task error provenance.

Revision ID: a3f4b5c6d7e8
Revises: f0a1b2c3d4e5
Create Date: 2026-08-13 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "a3f4b5c6d7e8"
down_revision: Union[str, Sequence[str], None] = "f0a1b2c3d4e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("errorresult", sa.Column("producer", sa.String(), nullable=True))
    op.add_column("errorresult", sa.Column("operation", sa.String(), nullable=True))
    op.add_column("errorresult", sa.Column("error_type", sa.String(), nullable=True))
    op.add_column("errorresult", sa.Column("cause_code", sa.String(), nullable=True))
    op.add_column(
        "errorresult",
        sa.Column("retry_scheduled", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column("errorresult", sa.Column("failed_attempt_number", sa.Integer(), nullable=True))
    op.create_index(
        "ix_errorresult_org_task_created_at",
        "errorresult",
        ["org_id", "task", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_errorresult_org_task_created_at", table_name="errorresult")
    op.drop_column("errorresult", "failed_attempt_number")
    op.drop_column("errorresult", "retry_scheduled")
    op.drop_column("errorresult", "cause_code")
    op.drop_column("errorresult", "error_type")
    op.drop_column("errorresult", "operation")
    op.drop_column("errorresult", "producer")
