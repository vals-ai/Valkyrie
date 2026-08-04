"""Models for hosted benchmark and task inspection."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import Field, field_serializer

from valkyrie.sdk.models._base import ResponseModel, serialize_utc
from valkyrie.sdk.models.run_tasks import (
    FetchTasksRequest,
    SingleTaskResponse,
    TaskArtifactsResponse,
    TasksResponse,
    TaskSummary,
)
from valkyrie.sdk.models.runs import BenchmarkStatus


class BenchmarkStatusEntry(ResponseModel):
    """Lightweight status and task counts for one run."""

    id: UUID
    status: BenchmarkStatus
    finished_at: datetime | None
    total_tasks: int
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    finished_tasks: int
    task_state_counts: dict[str, int] = Field(default_factory=dict)

    @field_serializer("finished_at")
    def serialize_finished_at(self, value: datetime | None) -> str | None:
        """Serialize an optional finish time with an explicit offset."""
        return serialize_utc(value)


class BenchmarkStatusResponse(ResponseModel):
    """Status entries returned for a group of runs."""

    entries: list[BenchmarkStatusEntry]


class SingleBenchmarkResponse(ResponseModel):
    """Hosted run detail used by benchmark inspection views."""

    id: UUID
    name: str
    agent_name: str
    model: str | None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    started_at: datetime
    finished_at: datetime | None
    status: BenchmarkStatus
    total_tasks: int
    finished_tasks: int
    task_state_counts: dict[str, int] = Field(default_factory=dict)
    started_by_email: str | None = None
    final_score: float | None = None
    error_message: str | None = None
    cloudwatch_url: str | None = None
    s3_bucket_url: str | None = None

    @field_serializer("started_at")
    def serialize_started_at(self, value: datetime) -> str:
        """Serialize the required start time with an explicit offset."""
        serialized = serialize_utc(value)
        assert serialized is not None
        return serialized

    @field_serializer("finished_at")
    def serialize_finished_at(self, value: datetime | None) -> str | None:
        """Serialize an optional finish time with an explicit offset."""
        return serialize_utc(value)


__all__ = [
    "BenchmarkStatusEntry",
    "BenchmarkStatusResponse",
    "FetchTasksRequest",
    "SingleBenchmarkResponse",
    "SingleTaskResponse",
    "TaskArtifactsResponse",
    "TasksResponse",
    "TaskSummary",
]
