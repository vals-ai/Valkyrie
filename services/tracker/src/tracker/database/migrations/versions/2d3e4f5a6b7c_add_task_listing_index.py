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
            SELECT
                i.indisvalid,
                i.indisunique,
                i.indnkeyatts = 3
                    AND i.indnatts = 3
                    AND i.indexprs IS NULL
                    AND i.indpred IS NULL
                    AND am.amname = 'btree'
                    AND (
                        SELECT array_agg(a.attname ORDER BY key.ordinality)
                        FROM unnest(i.indkey) WITH ORDINALITY AS key(attnum, ordinality)
                        JOIN pg_attribute AS a
                          ON a.attrelid = i.indrelid
                         AND a.attnum = key.attnum
                    ) = ARRAY['benchmark', 'org_id', 'started_at']::name[]
                    AND pg_index_column_has_property(i.indexrelid, 1, 'asc')
                    AND pg_index_column_has_property(i.indexrelid, 1, 'nulls_last')
                    AND pg_index_column_has_property(i.indexrelid, 2, 'asc')
                    AND pg_index_column_has_property(i.indexrelid, 2, 'nulls_last')
                    AND pg_index_column_has_property(i.indexrelid, 3, 'desc')
                    AND pg_index_column_has_property(i.indexrelid, 3, 'nulls_first')
                    AND t.relname = 'task'
                    AND tn.nspname = current_schema()
                    AS is_structurally_equivalent
            FROM pg_index AS i
            JOIN pg_class AS c ON c.oid = i.indexrelid
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            JOIN pg_class AS t ON t.oid = i.indrelid
            JOIN pg_namespace AS tn ON tn.oid = t.relnamespace
            JOIN pg_am AS am ON am.oid = c.relam
            WHERE c.relname = :index_name
              AND n.nspname = current_schema()
            """
            ),
            {"index_name": _INDEX_NAME},
        )
        .mappings()
        .one_or_none()
    )

    if (
        existing_index is not None
        and existing_index["indisvalid"]
        and not existing_index["indisunique"]
        and existing_index["is_structurally_equivalent"]
    ):
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
