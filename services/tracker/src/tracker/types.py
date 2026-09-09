"""API request and response types for the tracker service."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal, cast
from uuid import UUID

from benchmark_service.client import BenchmarkServiceClient
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator

from tracker.config import create_benchmark_service_url
from tracker.database.models import (
    AgentContractRequest,
    BenchmarkArguments,
    BenchmarkStatus,
    DocentReadingStatus,
    FinalEvaluation,
    TaskStatus,
)
from tracker.outbound_security import validate_benchmark_name, validate_service_url_syntax


def _serialize_utc(value: datetime | None) -> str | None:
    """Tag naive datetimes as UTC so JS clients parse them as UTC, not local."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


class BenchmarkDetails(BaseModel):
    status: BenchmarkStatus
    started_at: datetime
    total_tasks: int
    finished_tasks: int
    task_breakdown: dict[TaskStatus, int]
    docent_reading_status: DocentReadingStatus
    docent_reading_url: str | None = None


class AWSCredentials(BaseModel, frozen=True):
    aws_access_key_id: str = Field(repr=False)
    aws_secret_access_key: str = Field(repr=False)
    aws_default_region: str
    aws_session_token: str | None = Field(default=None, repr=False)


class HarnessConfig(BaseModel):
    aws: AWSCredentials
    s3_bucket: str
    log_group: str
    log_retention_policy: int
    sandbox_provider_secret_name: str


class StartBenchmarkRequest(BaseModel):
    contract: AgentContractRequest
    benchmark_name: str
    concurrency: int = 5
    label: str | None = None
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset: str | None = None
    harness_config: HarnessConfig | None = None
    custom_benchmark_service: str | None = None
    service_headers: dict[str, str] = Field(default_factory=dict, repr=False)
    sandbox_provider: str = "daytona"
    sandbox_provider_secret_name: str | None = None
    service_auth_header_name: str | None = None
    service_auth_secret_name: str | None = None
    webhook_secret_name: str | None = None
    webhook_intervals: list[int] | None = None

    @field_validator("benchmark_name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return validate_benchmark_name(value)

    @field_validator("custom_benchmark_service")
    @classmethod
    def validate_custom_service(cls, value: str | None) -> str | None:
        return validate_service_url_syntax(value) if value is not None else None

    @property
    def benchmark_service(self) -> BenchmarkServiceClient:
        from tracker.utils import create_benchmark_service_client

        # Prioritize user defined benchmark service over hosted one
        benchmark_service_url = self.custom_benchmark_service or create_benchmark_service_url(self.benchmark_name)
        return create_benchmark_service_client(
            url=benchmark_service_url,
            service_headers=self.service_headers,
        )


class FetchBenchmarkTasksRequest(BaseModel):
    benchmark_name: str
    dataset: str | None = None
    custom_benchmark_service: str | None = None
    service_headers: dict[str, str] = Field(default_factory=dict)

    @field_validator("benchmark_name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return validate_benchmark_name(value)

    @field_validator("custom_benchmark_service")
    @classmethod
    def validate_custom_service(cls, value: str | None) -> str | None:
        return validate_service_url_syntax(value) if value is not None else None


class StartBenchmarkErrorResponse(BaseModel):
    benchmark_id: UUID
    error_message: str


class StartBenchmarkResponse(BaseModel):
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


class FetchBenchmarkResponse(BaseModel):
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


class AverageTaskBreakdown(BaseModel):
    sandbox_build_duration: float | None
    agent_run_duration: float | None
    evaluation_run_duration: float | None
    sandbox_run_duration: float | None


class FinalViewResponse(BaseModel):
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


class S3UploadResultsResponse(BaseModel):
    s3_url: str
    presigned_url: str
    console_url: str
    expires_in: int = 86400


RetrieveResultsResponse = FinalViewResponse | S3UploadResultsResponse


_FORBIDDEN_MANAGED_AWS_KEYS = {
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "aws_profile",
    "x_harness_aws_access_key_id",
    "x_harness_aws_secret_access_key",
    "x_harness_aws_session_token",
    "x_harness_aws_profile",
}


def _contains_forbidden_managed_aws_key(value: object) -> bool:
    if isinstance(value, dict):
        for key, nested_value in cast(dict[object, object], value).items():
            normalized_key = str(key).lower().replace("-", "_")
            if normalized_key in _FORBIDDEN_MANAGED_AWS_KEYS or _contains_forbidden_managed_aws_key(nested_value):
                return True
    elif isinstance(value, list):
        return any(_contains_forbidden_managed_aws_key(item) for item in cast(list[object], value))
    return False


def validate_managed_execution_request(request: StartBenchmarkRequest) -> None:
    if request.harness_config is not None or _contains_forbidden_managed_aws_key(request.model_dump(mode="python")):
        raise ValueError("Managed execution cannot include AWS credentials")
    if not request.sandbox_provider or not request.sandbox_provider_secret_name:
        raise ValueError("Managed execution requires a sandbox provider and provider secret name")


class ManagedExecutionContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: Literal[2]
    benchmark_id: UUID
    verified_task_ids: list[str]
    start_benchmark_request: StartBenchmarkRequest

    @model_validator(mode="after")
    def validate_credential_free_request(self) -> "ManagedExecutionContext":
        validate_managed_execution_request(self.start_benchmark_request)
        return self


class AWSRuntimeResponse(BaseModel):
    mode: Literal["access_key", "managed"]
    region: str | None = None
    s3_bucket: str | None = None


class StatusResponse(BaseModel):
    status: str


class StopBenchmarkResponse(StatusResponse):
    pass


class RetryOrResumeBenchmarkResponse(StatusResponse):
    pass


class UpdateBenchmarkConcurrencyRequest(BaseModel):
    concurrency: int = Field(ge=1, strict=True)


class UpdateBenchmarkConcurrencyResponse(BaseModel):
    benchmark_id: UUID
    status: BenchmarkStatus
    concurrency: int


class Order(str, Enum):
    ASC = "asc"
    DESC = "desc"


class FetchBenchmarksRequest(BaseModel):
    agent_name: list[str] | None = None
    benchmark_name: list[str] | None = None
    model: str | None = None
    dataset: str | None = None
    label: str | None = None
    status: list[BenchmarkStatus] | None = None
    started_by: list[str] | None = None
    started_after: datetime | None = None
    started_before: datetime | None = None
    order_by: Order = Order.DESC  # Order is based off the time the benchmark was started at

    # Pagination — cursor preferred, offset/limit kept for backward compat
    cursor: str | None = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class BenchmarkTableRow(BaseModel):
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
    # Per-TaskStatus counts: {"PENDING": 1, "IN_PROGRESS": 2, "FINISHED": 4, ...}.
    # Absent keys mean zero; sum equals total_tasks.
    task_state_counts: dict[str, int] = Field(default_factory=dict)
    final_score: float | None = None
    error_message: str | None = None

    @field_serializer("started_at")
    def _serialize_started_at(self, value: datetime) -> str:
        result = _serialize_utc(value)
        assert result is not None  # started_at is non-nullable
        return result

    @field_serializer("finished_at")
    def _serialize_finished_at(self, value: datetime | None) -> str | None:
        return _serialize_utc(value)


class FetchBenchmarksResponse(BaseModel):
    benchmarks: list[BenchmarkTableRow]
    total_count: int | None = None
    next_cursor: str | None = None


class FetchBenchmarkMetadataResponse(BaseModel):
    benchmark_id: UUID
    benchmark_name: str
    benchmark_arguments: BenchmarkArguments
    started_by_email: str | None = None
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_uri: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None


class AnalyzeBenchmarkRequest(BaseModel):
    no_cache: bool = False
    # CLI resolves the analyzer Lambda from the agent's current pushed contract
    # and passes it here so the tracker doesn't have to parse the contract.
    lambda_function: str | None = None


class BenchmarkServiceEntry(BaseModel):
    name: str
    url: str
    auth_header_name: str | None = None
    auth_secret_name: str | None = None

    @field_validator("url")
    @classmethod
    def remove_trailing_slashes(cls, value: str) -> str:
        return validate_service_url_syntax(value)


class BenchmarkServiceHealth(BaseModel):
    name: str
    url: str
    healthy: bool
    latency_ms: int | None
    error: str | None = None


class BenchmarkServicesResponse(BaseModel):
    services: list[BenchmarkServiceHealth]


class BenchmarkServiceCatalogResponse(BaseModel):
    services: list[BenchmarkServiceEntry]


class BenchmarkServicesRequest(BaseModel):
    services: list[BenchmarkServiceEntry] = Field(default_factory=list)


class BenchmarkStatusEntry(BaseModel):
    id: UUID
    status: BenchmarkStatus
    finished_at: datetime | None
    total_tasks: int
    executor_release_id: str | None = None
    current_execution_release_id: str | None = None
    executor_artifact_digest: str | None = None
    executor_protocol_version: str | None = None
    finished_tasks: int
    # Per-TaskStatus counts; same shape as BenchmarkTableRow.task_state_counts.
    task_state_counts: dict[str, int] = Field(default_factory=dict)

    @field_serializer("finished_at")
    def _serialize_dt(self, value: datetime | None) -> str | None:
        return _serialize_utc(value)


class BenchmarkStatusResponse(BaseModel):
    entries: list[BenchmarkStatusEntry]


class SingleBenchmarkResponse(BaseModel):
    """Single-run view: BenchmarkTableRow fields plus optional final_score."""

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
    def _serialize_started_at(self, value: datetime) -> str:
        result = _serialize_utc(value)
        assert result is not None
        return result

    @field_serializer("finished_at")
    def _serialize_finished_at(self, value: datetime | None) -> str | None:
        return _serialize_utc(value)


class TaskSummary(BaseModel):
    id: UUID
    task_id: str
    status: TaskStatus
    started_at: datetime
    finished_at: datetime | None
    error_message: str | None = None

    @field_serializer("started_at")
    def _serialize_started_at(self, value: datetime) -> str:
        result = _serialize_utc(value)
        assert result is not None
        return result

    @field_serializer("finished_at")
    def _serialize_finished_at(self, value: datetime | None) -> str | None:
        return _serialize_utc(value)


class TasksResponse(BaseModel):
    tasks: list[TaskSummary]
    total_count: int


class SingleTaskResponse(BaseModel):
    id: UUID
    task_id: str
    status: TaskStatus
    started_at: datetime
    finished_at: datetime | None
    error_message: str | None
    evaluation_result: dict[str, Any] | None
    agent_caused_exit_reason: str | None

    @field_serializer("started_at")
    def _serialize_started_at(self, value: datetime) -> str:
        result = _serialize_utc(value)
        assert result is not None
        return result

    @field_serializer("finished_at")
    def _serialize_finished_at(self, value: datetime | None) -> str | None:
        return _serialize_utc(value)


class AgentEntry(BaseModel):
    name: str
    last_modified: str | None = None


class AgentsResponse(BaseModel):
    agents: list[AgentEntry]


class AgentDownloadURLResponse(BaseModel):
    name: str
    download_url: str
    expires_in: int


class TaskArtifactsResponse(BaseModel):
    cloudwatch_url: str | None
    agent_output_url: str | None
    agent_output_expires_in: int | None
