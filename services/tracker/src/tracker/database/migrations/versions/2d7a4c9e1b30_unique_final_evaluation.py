"""Enforce one final evaluation per benchmark.

Revision ID: 2d7a4c9e1b30
Revises: 6f3c2d9a8b10
Create Date: 2026-07-17 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "2d7a4c9e1b30"
down_revision: Union[str, Sequence[str], None] = "6f3c2d9a8b10"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    duplicates = (
        op.get_bind()
        .execute(
            sa.text(
                """
            SELECT benchmark, COUNT(*) AS row_count
            FROM finalevaluation
            GROUP BY benchmark
            HAVING COUNT(*) > 1
            ORDER BY benchmark
            """
            )
        )
        .all()
    )
    if duplicates:
        audit = ", ".join(f"{benchmark} ({row_count} rows)" for benchmark, row_count in duplicates)
        raise RuntimeError(f"Resolve duplicate final evaluations before migrating: {audit}")

    op.create_unique_constraint(
        "unique_final_evaluation_per_benchmark",
        "finalevaluation",
        ["benchmark"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "unique_final_evaluation_per_benchmark",
        "finalevaluation",
        type_="unique",
    )
