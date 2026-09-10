"""Organization-scoped sandbox scheduler snapshots."""

from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import Field, field_serializer

from valkyrie.sdk.models._base import ResponseModel, serialize_utc


class SchedulerSummaryResponse(ResponseModel):
    """Total waiting and active task counts, independent of entry limits."""

    waiting: int = 0
    building: int = 0
    in_progress: int = 0
    evaluating: int = 0


class SchedulerActiveStatus(str, Enum):
    """Task states included in the scheduler's active entries."""

    BUILDING = "BUILDING"
    IN_PROGRESS = "IN_PROGRESS"
    EVALUATING = "EVALUATING"


class SchedulerPoolResponse(ResponseModel):
    """Waiting task count for one sandbox admission pool."""

    pool_id: str
    waiting: int


class SchedulerWaitingEntryResponse(ResponseModel):
    """One waiting task with its priority and position within its pool."""

    benchmark_uuid: UUID
    task_uuid: UUID
    benchmark_name: str
    external_task_id: str
    started_by_email: str | None = None
    pool_id: str
    position: int
    priority: int
    enqueued_at: datetime

    @field_serializer("enqueued_at")
    def serialize_enqueued_at(self, value: datetime) -> str:
        """Serialize the enqueue time with an explicit offset."""
        serialized = serialize_utc(value)
        assert serialized is not None
        return serialized


class SchedulerActiveEntryResponse(ResponseModel):
    """One task currently building, running, or evaluating."""

    benchmark_uuid: UUID
    task_uuid: UUID
    benchmark_name: str
    external_task_id: str
    started_by_email: str | None = None
    status: SchedulerActiveStatus
    started_at: datetime

    @field_serializer("started_at")
    def serialize_started_at(self, value: datetime) -> str:
        """Serialize the start time with an explicit offset."""
        serialized = serialize_utc(value)
        assert serialized is not None
        return serialized


class SchedulerOverviewResponse(ResponseModel):
    """Scheduler totals and bounded task entries for the authenticated organization."""

    observed_at: datetime
    summary: SchedulerSummaryResponse
    pools: list[SchedulerPoolResponse]
    waiting_entries: list[SchedulerWaitingEntryResponse]
    active_entries: list[SchedulerActiveEntryResponse]
    waiting_capped: bool
    active_capped: bool
    waiting_next_offset: int | None = Field(description="Next waiting offset, or null when this list is exhausted.")
    active_next_offset: int | None = Field(description="Next active offset, or null when this list is exhausted.")

    @field_serializer("observed_at")
    def serialize_observed_at(self, value: datetime) -> str:
        """Serialize the observation time with an explicit offset."""
        serialized = serialize_utc(value)
        assert serialized is not None
        return serialized
