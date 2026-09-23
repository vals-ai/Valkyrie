"""Stable wire models, independent of Tracker's ORM and configuration."""

from datetime import datetime
from enum import Enum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue


class DispatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claimant_id: UUID


class ClaimRequest(DispatchRequest):
    benchmark_id: UUID
    executor_release_id: str = Field(min_length=1)
    executor_artifact_uri: str = Field(min_length=1)
    executor_artifact_digest: str = Field(pattern="^[0-9a-f]{64}$")
    executor_protocol_version: str = Field(min_length=1)


class FailRequest(DispatchRequest):
    error_message: str = Field(min_length=1, max_length=4096)


class LeaseResponse(BaseModel):
    dispatch_id: UUID
    claimant_id: UUID
    lease_expires_at: datetime
    server_time: datetime


class AuthorityResponse(BaseModel):
    current: bool


class TerminalResponse(BaseModel):
    dispatch_id: UUID
    status: Literal["FINISHED", "FAILED"]
    finished_at: datetime


class RunStatus(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    BUILDING = "BUILDING"
    IN_PROGRESS = "IN_PROGRESS"
    EVALUATING = "EVALUATING"
    STOPPED = "STOPPED"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


class RunTasksRequest(DispatchRequest):
    task_ids: list[str] = Field(default_factory=list, max_length=1000)
    include_eval_resume_state: bool = False


class RunResources(BaseModel):
    region: str
    s3_bucket: str
    log_group: str
    log_retention_days: int


class RunInfo(BaseModel):
    benchmark_id: UUID
    org_id: UUID
    org_name: str
    benchmark_name: str
    agent_name: str
    model: str | None
    started_at: datetime
    status: RunStatus
    aws_managed: bool
    concurrency: int
    queue_pool_id: str | None
    resources: RunResources | None
    started_by_email: str | None = None


class TaskState(BaseModel):
    id: UUID
    task_id: str
    status: TaskStatus
    started_at: datetime
    finished_at: datetime | None
    eval_resume_state: dict[str, JsonValue] | None = None


class RunStateResponse(BaseModel):
    current: bool
    run: RunInfo
    tasks: list[TaskState]
    task_counts: dict[TaskStatus, int]
