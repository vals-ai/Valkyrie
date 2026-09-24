"""Add external-service accounting fields to task breakdown.

Revision ID: 7b8c9d0e1f2a
Revises: 6a7b8c9d0e1f
Create Date: 2026-09-19 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "7b8c9d0e1f2a"
down_revision: Union[str, Sequence[str], None] = "2d3e4f5a6b7c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("taskbreakdown", sa.Column("accounting_session_id", sa.String(), nullable=True))
    op.add_column("taskbreakdown", sa.Column("base_generation_allowance_seconds", sa.Float(), nullable=True))
    op.add_column("taskbreakdown", sa.Column("cumulative_time_credit_cap_seconds", sa.Float(), nullable=True))
    op.add_column("taskbreakdown", sa.Column("external_service_overhead_seconds", sa.Float(), nullable=True))
    op.add_column("taskbreakdown", sa.Column("external_service_credit_applied_seconds", sa.Float(), nullable=True))
    op.add_column("taskbreakdown", sa.Column("effective_generation_allowance_seconds", sa.Float(), nullable=True))
    op.add_column("taskbreakdown", sa.Column("external_service_credit_revision", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("taskbreakdown", "external_service_credit_revision")
    op.drop_column("taskbreakdown", "effective_generation_allowance_seconds")
    op.drop_column("taskbreakdown", "external_service_credit_applied_seconds")
    op.drop_column("taskbreakdown", "external_service_overhead_seconds")
    op.drop_column("taskbreakdown", "cumulative_time_credit_cap_seconds")
    op.drop_column("taskbreakdown", "base_generation_allowance_seconds")
    op.drop_column("taskbreakdown", "accounting_session_id")