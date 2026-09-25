"""Version-one provider creation reservations, independent of database models."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from tracker.executor_api.v1.task_schemas import TaskAttemptRequest


class ReservePoolRequest(TaskAttemptRequest):
    expected_revision: int = Field(ge=0)


class ReservePoolResponse(BaseModel):
    command_id: UUID
    reserved: bool
    reservation_id: UUID | None
    revision: int | None


class ReleasePoolRequest(TaskAttemptRequest):
    """Confirm creation and any required cleanup have settled before releasing."""

    reservation_id: UUID


class ReleasePoolResponse(BaseModel):
    command_id: UUID
    reservation_id: UUID
    released: Literal[True] = True
