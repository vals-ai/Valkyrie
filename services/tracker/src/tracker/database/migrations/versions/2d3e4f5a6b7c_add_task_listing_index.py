"""Add task listing index.

Revision ID: 2d3e4f5a6b7c
Revises: 1c2d3e4f5a6b
Create Date: 2026-09-21 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "2d3e4f5a6b7c"
down_revision: Union[str, Sequence[str], None] = "1c2d3e4f5a6b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_INDEX_NAME = "ix_task_benchmark_org_started_at"


def upgrade() -> None:
    existing_index = (
        op.get_bind()
        .execute(
            sa.text(
                """
            SELECT indexrelid::regclass::text, indisvalid
            FROM pg_index
            JOIN pg_class ON pg_class.oid = pg_index.indexrelid
            JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            WHERE pg_class.relname = :index_name
              AND pg_namespace.nspname = current_schema()
            """
            ),
            {"index_name": _INDEX_NAME},
        )
        .mappings()
        .one_or_none()
    )

    if existing_index is not None and existing_index["indisvalid"]:
        return

    with op.get_context().autocommit_block():
        if existing_index is not None:
            op.execute(sa.text(f'DROP INDEX CONCURRENTLY IF EXISTS "{_INDEX_NAME}"'))
        op.create_index(
            _INDEX_NAME,
            "task",
            ["benchmark", "org_id", sa.text("started_at DESC")],
            unique=False,
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text(f'DROP INDEX CONCURRENTLY IF EXISTS "{_INDEX_NAME}"'))
