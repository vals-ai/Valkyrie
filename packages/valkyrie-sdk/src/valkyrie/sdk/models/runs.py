"""Request and response models for run lifecycle operations."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_serializer

from valkyrie.sdk.models._base import ResponseModel, serialize_utc
from valkyrie.sdk.models.agents import AgentContractRequest
from valkyrie.sdk.models.config import HarnessConfig


class TaskStatus(str, Enum):
    """Lifecycle states for one benchmark task."""

    PENDING = "PENDING"
    BUILDING = "BUILDING"
    IN_PROGRESS = "IN_PROGRESS"
    EVALUATING = "EVALUATING"
    STOPPED = "STOPPED"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


class BenchmarkStatus(str, Enum):
    """Lifecycle states for a benchmark run."""

    IN_PROGRESS = "IN_PROGRESS"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


class DocentReadingStatus(str, Enum):
    """Lifecycle states for generated Docent analysis."""

    IDLE = "IDLE"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    DONE = "DONE"


class RetryMode(str, Enum):
    """Supported retry strategies."""

    AUTO = "auto"
    FROM_SCRATCH = "from_scratch"


class Order(str, Enum):
    """Sort order for run listings."""

    ASC = "asc"
    DESC = "desc"


class StartBenchmarkRequest(BaseModel):
    """Wire payload used to start a benchmark run."""

    contract: AgentContractRequest
    benchmark_name: str
    concurrency: int = 5
    label: str | None = None
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset: str | None = None
    harness_config: HarnessConfig
    custom_benchmark_service: str | None = None
    service_headers: dict[str, str] = Field(default_factory=dict, repr=False)
    sandbox_provider: str = "daytona"
    sandbox_provider_secret_name: str | None = None
    service_auth_header_name: str | None = None
    service_auth_secret_name: str | None = None
    webhook_secret_name: str | None = None
    webhook_intervals: list[int] | None = None


class FetchBenchmarksRequest(BaseModel):
    """Filters and pagination for listing benchmark runs."""

    agent_name: list[str] | None = None
    benchmark_name: list[str] | None = None
    model: str | None = None
    dataset: str | None = None
    label: str | None = None
    status: list[BenchmarkStatus] | None = None
    started_by: list[str] | None = None
    started_after: datetime | None = None
    started_before: datetime | None = None
    order_by: Order = Order.DESC
    cursor: str | None = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class AnalyzeBenchmarkRequest(BaseModel):
    """Wire payload used to trigger Docent analysis for a run."""

    no_cache: bool = False
    lambda_function: str | None = None


class AnalyzeEvent(ResponseModel):
    """One progress or completion event from run analysis."""

    event: str
    data: dict[str, Any]


class BenchmarkDetails(ResponseModel):
    """Detailed progress for a fetched run."""

    status: BenchmarkStatus
    started_at: datetime
    total_tasks: int
    finished_tasks: int
    task_breakdown: dict[TaskStatus, int]
    docent_reading_status: DocentReadingStatus
    docent_reading_url: str | None = None


class StartBenchmarkResponse(ResponseModel):
    """Response returned after starting a run."""

    benchmark_name: str
    agent_name: str
    benchmark_id: UUID
    concurrency: int
    started_at: datetime
    task_count: int
    cloudwatch_url: str
    s3_bucket_url: str
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None


class FetchBenchmarkResponse(ResponseModel):
    """Current state of one benchmark run."""

    benchmark_name: str
    benchmark_id: UUID
    details: BenchmarkDetails
    s3_bucket_url: str
    label: str | None = None
    final_score: float | None = None
    error_message: str | None = None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None


class BenchmarkTableRow(ResponseModel):
    """Summary row returned by the run-list endpoint."""

    id: UUID
    name: str
    agent_name: str
    label: str | None = None
    model: str | None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    dataset: str = "default"
    started_by_email: str | None
    started_at: datetime
    finished_at: datetime | None
    status: BenchmarkStatus
    total_tasks: int
    finished_tasks: int
    task_state_counts: dict[str, int] = Field(default_factory=dict)
    final_score: float | None = None
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


class FetchBenchmarksResponse(ResponseModel):
    """Page of benchmark runs."""

    benchmarks: list[BenchmarkTableRow]
    total_count: int | None = None
    next_cursor: str | None = None


class BenchmarkArguments(ResponseModel):
    """Arguments retained with a completed run."""

    contract: AgentContractRequest
    concurrency: int
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset: str | None = None
    sandbox_provider: str = "daytona"
    sandbox_provider_secret_name: str | None = None


class FetchBenchmarkMetadataResponse(ResponseModel):
    """Stored launch metadata for one run."""

    benchmark_id: UUID
    benchmark_name: str
    benchmark_arguments: BenchmarkArguments
    started_by_email: str | None = None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_uri: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None


class FinalEvaluation(ResponseModel):
    """Final aggregate evaluation stored for a run."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    org_id: str
    benchmark: str
    final_score: float
    properties: dict[str, Any] = Field(default_factory=dict)


class AverageTaskBreakdown(ResponseModel):
    """Average timing breakdown across completed tasks."""

    sandbox_build_duration: float | None
    agent_run_duration: float | None
    evaluation_run_duration: float | None
    sandbox_run_duration: float | None


class FinalViewResponse(ResponseModel):
    """Inline final results for a benchmark run."""

    benchmark_id: UUID
    benchmark_name: str
    started_at: datetime
    finished_at: datetime | None
    status: BenchmarkStatus
    error_message: str | None
    benchmark_arguments: BenchmarkArguments
    tasks_stopped: int | None
    final_evaluation: FinalEvaluation | None
    average_task_breakdown: AverageTaskBreakdown | None
    evaluation_results: dict[str, dict[str, Any]] | None
    task_errors: dict[str, str] | None


class S3UploadResultsResponse(ResponseModel):
    """URLs returned when final results are uploaded to S3."""

    s3_url: str
    presigned_url: str
    console_url: str


class ResultsExistResponse(ResponseModel):
    """Whether the canonical result file already exists in S3."""

    exists: bool


RetrieveResultsResponse = FinalViewResponse | S3UploadResultsResponse


class StatusResponse(ResponseModel):
    """Generic status response."""

    status: str


class StopBenchmarkResponse(StatusResponse):
    """Response returned after stopping a run."""


class RetryOrResumeBenchmarkResponse(StatusResponse):
    """Response returned after retrying or resuming a run."""
