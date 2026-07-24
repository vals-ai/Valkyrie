"""Add durable mutation operation receipts."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d6e7f8a9b0c1"
down_revision: str | Sequence[str] | None = "c8a1d4e7f2b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

operation_state = sa.Enum(
    "processing",
    "succeeded",
    "failed",
    "uncertain",
    name="mutationoperationstate",
)
operation_kind = sa.Enum(
    "analyze_benchmark",
    "start_benchmark",
    "stop_benchmark",
    "retry_or_resume_benchmark",
    name="mutationoperationkind",
)


def upgrade() -> None:
    op.create_table(
        "mutationoperation",
        sa.Column("org_id", sa.Uuid(), nullable=False),
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("kind", operation_kind, nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("state", operation_state, nullable=False),
        sa.Column("response", sa.JSON(none_as_null=True), nullable=True),
        sa.Column("failure_status_code", sa.Integer(), nullable=True),
        sa.Column("failure_detail", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint(
            "("
            "state = 'succeeded' AND response IS NOT NULL "
            "AND failure_status_code IS NULL AND failure_detail IS NULL"
            ") OR ("
            "state = 'failed' AND response IS NULL "
            "AND failure_status_code IS NOT NULL AND failure_detail IS NOT NULL"
            ") OR ("
            "state IN ('processing', 'uncertain') AND response IS NULL "
            "AND failure_status_code IS NULL AND failure_detail IS NULL"
            ")",
            name="mutation_operation_state_payload",
        ),
        sa.CheckConstraint(
            "length(request_fingerprint) = 64",
            name="mutation_operation_fingerprint_is_sha256",
        ),
        sa.CheckConstraint(
            "length(failure_detail) <= 4000",
            name="mutation_operation_failure_detail_is_bounded",
        ),
        sa.ForeignKeyConstraint(["org_id"], ["org.id"]),
        sa.PrimaryKeyConstraint("org_id", "operation_id"),
    )


def downgrade() -> None:
    op.drop_table("mutationoperation")
    operation_kind.drop(op.get_bind(), checkfirst=True)
    operation_state.drop(op.get_bind(), checkfirst=True)
