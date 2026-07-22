"""Models for hosted benchmark and task inspection."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_serializer

from valkyrie.sdk.models._base import ResponseModel, serialize_utc
from valkyrie.sdk.models.runs import BenchmarkStatus, Order, TaskStatus


class FetchTasksRequest(BaseModel):
    """Filters, sorting, and pagination for a benchmark's tasks."""

    status: list[TaskStatus] | None = None
    task_id_search: str | None = None
    sort: Literal["task_id", "started_at", "duration", "status"] = "started_at"
    sort_dir: Order = Order.DESC
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class BenchmarkStatusEntry(ResponseModel):
    """Lightweight status and task counts for one run."""

    id: UUID
    status: BenchmarkStatus
    finished_at: datetime | None
    total_tasks: int
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


class TaskSummary(ResponseModel):
    """Summary information for one task in a paginated run."""

    id: UUID
    task_id: str
    status: TaskStatus
    started_at: datetime
    finished_at: datetime | None
    error_message: str | None = None

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


class TasksResponse(ResponseModel):
    """Paginated task summaries for one run."""

    tasks: list[TaskSummary]
    total_count: int


class SingleTaskResponse(ResponseModel):
    """Detailed state and evaluation output for one task."""

    id: UUID
    task_id: str
    status: TaskStatus
    started_at: datetime
    finished_at: datetime | None
    error_message: str | None
    evaluation_result: dict[str, Any] | None
    agent_caused_exit_reason: str | None

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


class TaskArtifactsResponse(ResponseModel):
    """Temporary artifact and log links for one task."""

    cloudwatch_url: str | None
    agent_output_url: str | None
    agent_output_expires_in: int | None
