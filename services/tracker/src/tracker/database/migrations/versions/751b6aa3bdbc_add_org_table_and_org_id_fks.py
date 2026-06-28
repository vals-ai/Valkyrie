"""add org table and org_id fks

Revision ID: 751b6aa3bdbc
Revises: c1f2af2104e1
Create Date: 2026-03-27 00:00:00.000000

"""

from typing import Sequence, Union
from uuid import uuid4

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "751b6aa3bdbc"
down_revision: Union[str, Sequence[str], None] = "fc0b597e5045"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

DEFAULT_ORG_NAME = "default"


def upgrade() -> None:
    default_org_id = str(uuid4())

    # 1. Create org table
    op.create_table(
        "org",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_org_name"), "org", ["name"], unique=True)

    # 2. Insert default org
    op.execute(
        sa.text("INSERT INTO org (id, name) VALUES (:id, :name)").bindparams(id=default_org_id, name=DEFAULT_ORG_NAME)
    )

    # 3. Add org_id as nullable to all tables
    for table in ("benchmark", "task", "finalevaluation", "evaluationresult"):
        op.add_column(table, sa.Column("org_id", sa.Uuid(), nullable=True))

    # 4. Backfill all existing rows
    for table in ("benchmark", "task", "finalevaluation", "evaluationresult"):
        op.execute(sa.text(f"UPDATE {table} SET org_id = :org_id").bindparams(org_id=default_org_id))

    # 5. Set NOT NULL + FK constraints
    for table in ("benchmark", "task", "finalevaluation", "evaluationresult"):
        op.alter_column(table, "org_id", nullable=False)
        op.create_foreign_key(f"fk_{table}_org_id", table, "org", ["org_id"], ["id"])


def downgrade() -> None:
    for table in ("evaluationresult", "finalevaluation", "task", "benchmark"):
        op.drop_constraint(f"fk_{table}_org_id", table, type_="foreignkey")
        op.drop_column(table, "org_id")
    op.drop_index(op.f("ix_org_name"), table_name="org")
    op.drop_table("org")
