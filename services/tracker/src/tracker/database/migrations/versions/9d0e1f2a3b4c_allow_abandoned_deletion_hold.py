"""Allow an explicitly abandoned deletion hold to release its run."""

from alembic import op
from alembic.util import CommandError

revision = "9d0e1f2a3b4c"
down_revision = "8c9d0e1f2a3b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("deletion_hold_permanent", "runlifecycle", type_="check")
    op.create_check_constraint(
        "deletion_hold_permanent",
        "runlifecycle",
        "purpose != 'deletion' OR released_at IS NULL OR phase = 'abandoned'",
    )


def downgrade() -> None:
    raise CommandError("Abandoned deletion holds must stay releasable; roll forward")
