"""Replace agent_timed_out bool with agent_caused_exit_reason enum

Revision ID: a9d2e1c8b3f4
Revises: 751b6aa3bdbc
Create Date: 2026-04-16 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a9d2e1c8b3f4"
down_revision: Union[str, Sequence[str], None] = "751b6aa3bdbc"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_ENUM_NAME = "agentcausedexitreason"
_ENUM_VALUES = ("timeout", "os_killed")


def upgrade() -> None:
    """Upgrade schema."""
    exit_reason_enum = sa.Enum(*_ENUM_VALUES, name=_ENUM_NAME)
    exit_reason_enum.create(op.get_bind(), checkfirst=True)

    op.add_column(
        "evaluationresult",
        sa.Column("agent_caused_exit_reason", exit_reason_enum, nullable=True),
    )

    # Backfill: existing agent_timed_out=true rows map to the timeout reason.
    # agent_timed_out=false rows remain NULL (clean exit).
    op.execute(
        "UPDATE evaluationresult SET agent_caused_exit_reason = 'timeout' WHERE agent_timed_out = true"
    )

    op.drop_column("evaluationresult", "agent_timed_out")


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column(
        "evaluationresult",
        sa.Column("agent_timed_out", sa.Boolean(), nullable=True),
    )

    # Only rows whose exit reason was a timeout are reconstructed as timed-out.
    # Everything else (NULL or os_killed) becomes false — os_killed has no bool equivalent.
    op.execute(
        "UPDATE evaluationresult SET agent_timed_out = (agent_caused_exit_reason = 'timeout')"
    )
    op.alter_column("evaluationresult", "agent_timed_out", nullable=False)

    op.drop_column("evaluationresult", "agent_caused_exit_reason")

    sa.Enum(name=_ENUM_NAME).drop(op.get_bind(), checkfirst=True)
