"""Add sealed dispatch inputs and the launched ECS task ARN.

Revision ID: 3e4f5a6b7c8d
Revises: 2d3e4f5a6b7c
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from alembic.util import CommandError

revision: str = "3e4f5a6b7c8d"
down_revision: Union[str, Sequence[str], None] = "2d3e4f5a6b7c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "executor_dispatch_payload",
        sa.Column("dispatch_id", sa.Uuid(), nullable=False),
        sa.Column("ciphertext", sa.LargeBinary(), nullable=False),
        sa.Column("encrypted_data_key", sa.LargeBinary(), nullable=False),
        sa.Column("nonce", sa.LargeBinary(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["dispatch_id"], ["executordispatch.id"]),
        sa.PrimaryKeyConstraint("dispatch_id"),
    )
    op.add_column("executordispatch", sa.Column("ecs_task_arn", sa.String(), nullable=True))


def downgrade() -> None:
    raise CommandError("Dispatch payload migration is irreversible; retain the additive schema and roll forward")
