"""Add durable lifecycle holds and positive host process-exit receipts."""

import sqlalchemy as sa
from alembic import op
from alembic.util import CommandError

revision = "7b8c9d0e1f2a"
down_revision = "6a7b8c9d0e1f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("executordispatch", sa.Column("process_exited_at", sa.DateTime(), nullable=True))
    op.create_table(
        "runlifecycle",
        sa.Column("run_id", sa.Uuid(), primary_key=True),
        sa.Column("identity_json", sa.String(), nullable=False),
        sa.Column("scope_json", sa.String(), nullable=False),
        sa.Column("purpose", sa.String(), nullable=False),
        sa.Column("phase", sa.String(), nullable=False),
        sa.Column("acquired_at", sa.DateTime(), nullable=False),
        sa.Column("released_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint("purpose IN ('relocation', 'deletion')", name="lifecycle_purpose"),
        sa.CheckConstraint("purpose != 'deletion' OR released_at IS NULL", name="deletion_hold_permanent"),
    )


def downgrade() -> None:
    raise CommandError("Lifecycle fences and exit evidence must be retained; roll forward")
