"""Record invitations to join organizations until the invitee accepts.

Revision ID: 6a36c1a30b60
Revises: c2e7039fa933
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "6a36c1a30b60"
down_revision: str | Sequence[str] | None = "c2e7039fa933"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "organization_invite",
        sa.Column("organization", sa.String(), nullable=False),
        sa.Column("email", sa.String(), nullable=False),
        sa.Column("invited_by", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("organization", "email"),
    )
    op.create_index("ix_organization_invite_email", "organization_invite", ["email"])


def downgrade() -> None:
    op.drop_index("ix_organization_invite_email", table_name="organization_invite")
    op.drop_table("organization_invite")
