"""Record each organization's storage key, the name of its S3 bucket.

Revision ID: a7b8c9d0e1f2
Revises: b3c4d5e6f7a8
Create Date: 2026-10-06 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

from tracker import config

revision: str = "a7b8c9d0e1f2"
down_revision: Union[str, Sequence[str], None] = "b3c4d5e6f7a8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_VALS_ORG_NAME = "vals.ai"


def upgrade() -> None:
    """Add the storage key column; Vals keeps the deployment bucket it already uses."""
    op.add_column("org", sa.Column("storage_key", sa.String(), nullable=True))
    op.create_unique_constraint("org_storage_key_key", "org", ["storage_key"])
    if config.AWS_DEPLOYMENT_S3_BUCKET:
        op.execute(
            sa.text("UPDATE org SET storage_key = :bucket WHERE name = :name").bindparams(
                bucket=config.AWS_DEPLOYMENT_S3_BUCKET, name=_VALS_ORG_NAME
            )
        )


def downgrade() -> None:
    op.drop_constraint("org_storage_key_key", "org", type_="unique")
    op.drop_column("org", "storage_key")
