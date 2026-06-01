"""API request and response types for the tracker service."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from tracker.database.models import OrgConfig

from benchmark_service.client import BenchmarkServiceClient

from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_serializer, field_validator


def _serialize_utc(value: datetime | None) -> str | None:
    """Tag naive datetimes as UTC so JS clients parse them as UTC, not local."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


from tracker.config import create_benchmark_service_url
from tracker.database.models import (
    AgentContractRequest,
    BenchmarkArguments,
    BenchmarkStatus,
    FinalEvaluation,
    TaskStatus,
)


class BenchmarkDetails(BaseModel):
    status: BenchmarkStatus
    started_at: datetime
    total_tasks: int
    finished_tasks: int
    task_breakdown: dict[TaskStatus, int]


class AWSCredentials(BaseModel, frozen=True):
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_default_region: str

    @classmethod
    def from_org_config(cls, cfg: "OrgConfig") -> "AWSCredentials":
        return cls(
            aws_access_key_id=cfg.aws_access_key_id,
            aws_secret_access_key=cfg.aws_secret_access_key,
            aws_default_region=cfg.aws_default_region,
        )


class HarnessConfig(BaseModel):
    aws: AWSCredentials
    s3_bucket: str
    log_group: str
    log_retention_policy: int
    daytona_secret_name: str


class StartBenchmarkRequest(BaseModel):
    contract: AgentContractRequest
    benchmark_name: str
    concurrency: int = 5
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset: str | None = None
    harness_config: HarnessConfig
    custom_benchmark_service: str | None = None
    service_headers: dict[str, str] = {}
    webhook_secret_name: str | None = None
    webhook_intervals: list[int] | None = None

    @property
    def benchmark_service(self) -> BenchmarkServiceClient:
        from tracker.utils import create_benchmark_service_client

        # Prioritize user defined benchmark service over hosted one
        benchmark_service_url = self.custom_benchmark_service or create_benchmark_service_url(self.benchmark_name)
        return create_benchmark_service_client(
            url=benchmark_service_url,
            daytona_secret_name=self.harness_config.daytona_secret_name,
            aws=self.harness_config.aws,
            service_headers=self.service_headers,
        )


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


class FetchBenchmarkResponse(BaseModel):
    benchmark_name: str
    benchmark_id: UUID
    details: BenchmarkDetails
    s3_bucket_url: str


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
    evaluation_results: dict[str, dict[str, Any]] | None
    task_errors: dict[str, str] | None


class S3UploadResultsResponse(BaseModel):
    s3_url: str
    presigned_url: str
    console_url: str


RetrieveResultsResponse = FinalViewResponse | S3UploadResultsResponse


class StatusResponse(BaseModel):
    status: str


class StopBenchmarkResponse(StatusResponse):
    pass


class RetryOrResumeBenchmarkResponse(StatusResponse):
    pass


class Order(str, Enum):
    ASC = "asc"
    DESC = "desc"


class FetchBenchmarksRequest(BaseModel):
    # CSV-valued filters: "FINISHED,IN_PROGRESS" → multiple values; empty/absent → no filter.
    agent_name: str | None = None
    benchmark_name: str | None = None
    model: str | None = None
    status: str | None = None
    run_by_user_id: str | None = None
    started_after: datetime | None = None
    started_before: datetime | None = None
    order_by: Order = Order.DESC  # Order is based off the time the benchmark was started at

    # Pagination — cursor preferred, offset/limit kept for backward compat
    cursor: str | None = None
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)

    def parsed_statuses(self) -> list[BenchmarkStatus]:
        """Return the list of BenchmarkStatus values from the CSV status field."""
        if not self.status:
            return []
        result: list[BenchmarkStatus] = []
        for token in self.status.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                result.append(BenchmarkStatus(token))
            except ValueError:
                continue
        return result

    def parsed_run_by_user_ids(self) -> list[UUID]:
        """Return parsed UUIDs from the CSV run_by_user_id field."""
        if not self.run_by_user_id:
            return []
        result: list[UUID] = []
        for token in self.run_by_user_id.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                result.append(UUID(token))
            except ValueError:
                continue
        return result

    def parsed_benchmark_names(self) -> list[str]:
        """Return the list of benchmark names from the CSV benchmark_name field."""
        if not self.benchmark_name:
            return []
        return [t.strip() for t in self.benchmark_name.split(",") if t.strip()]

    def parsed_agent_names(self) -> list[str]:
        """Return the list of agent names from the CSV agent_name field."""
        if not self.agent_name:
            return []
        return [t.strip() for t in self.agent_name.split(",") if t.strip()]


class BenchmarkTableRow(BaseModel):
    id: UUID
    name: str
    agent_name: str
    model: str | None
    started_at: datetime
    finished_at: datetime | None
    status: BenchmarkStatus
    total_tasks: int
    finished_tasks: int
    # Per-TaskStatus counts: {"PENDING": 1, "IN_PROGRESS": 2, "FINISHED": 4, ...}.
    # Absent keys mean zero; sum equals total_tasks.
    task_state_counts: dict[str, int] = {}
    run_by_email: str | None = None
    final_score: float | None = None

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


MASKED_SECRET = "********"


class BenchmarkServiceEntry(BaseModel):
    name: str
    url: str
    auth_header_name: str | None = None
    auth_secret_name: str | None = None


class BenchmarkServiceHealth(BaseModel):
    name: str
    url: str
    healthy: bool
    latency_ms: int | None
    error: str | None = None


class BenchmarkServicesResponse(BaseModel):
    services: list[BenchmarkServiceHealth]


class OrgConfigResponse(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_default_region: str
    s3_bucket: str
    daytona_secret_name: str
    log_group: str | None = None
    log_retention_policy: str | None = None
    webhook: str | None = None
    benchmark_services: list[BenchmarkServiceEntry] = []

    @classmethod
    def from_org_config(cls, config: OrgConfig) -> OrgConfigResponse:
        return cls(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=MASKED_SECRET,
            aws_default_region=config.aws_default_region,
            s3_bucket=config.s3_bucket,
            daytona_secret_name=MASKED_SECRET,
            log_group=config.log_group,
            log_retention_policy=config.log_retention_policy,
            webhook=MASKED_SECRET if config.webhook is not None else None,
            benchmark_services=[BenchmarkServiceEntry(**s) for s in (config.benchmark_services or [])],
        )


class BenchmarkStatusEntry(BaseModel):
    id: UUID
    status: BenchmarkStatus
    finished_at: datetime | None
    total_tasks: int
    finished_tasks: int
    # Per-TaskStatus counts; same shape as BenchmarkTableRow.task_state_counts.
    task_state_counts: dict[str, int] = {}

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
    started_at: datetime
    finished_at: datetime | None
    status: BenchmarkStatus
    total_tasks: int
    finished_tasks: int
    task_state_counts: dict[str, int] = {}
    run_by_email: str | None = None
    final_score: float | None = None

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


class LogEvent(BaseModel):
    timestamp: int  # ms since epoch
    message: str
    log_stream: str


class LogsResponse(BaseModel):
    events: list[LogEvent]
    next_token: str | None = None


class OrgConfigUpdate(BaseModel):
    model_config = {"extra": "forbid"}

    aws_access_key_id: str
    aws_secret_access_key: str
    aws_default_region: str
    s3_bucket: str
    daytona_secret_name: str
    log_group: str | None = None
    log_retention_policy: str | None = None
    webhook: str | None = None
    benchmark_services: list[BenchmarkServiceEntry] = []

    @field_validator("webhook")
    @classmethod
    def _validate_webhook_scheme(cls, value: str | None) -> str | None:
        if value is not None and value != MASKED_SECRET:
            parsed = urlparse(value)
            if parsed.scheme != "https":
                raise ValueError("webhook URL must use https://")
        return value

    def apply_to(self, config: OrgConfig) -> None:
        config.aws_access_key_id = self.aws_access_key_id
        if self.aws_secret_access_key != MASKED_SECRET:
            config.aws_secret_access_key = self.aws_secret_access_key
        config.aws_default_region = self.aws_default_region
        config.s3_bucket = self.s3_bucket
        if self.daytona_secret_name != MASKED_SECRET:
            config.daytona_secret_name = self.daytona_secret_name
        config.log_group = self.log_group
        config.log_retention_policy = self.log_retention_policy
        if self.webhook != MASKED_SECRET:
            config.webhook = self.webhook
        config.benchmark_services = [s.model_dump() for s in self.benchmark_services]


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


class FileEntry(BaseModel):
    key: str
    size: int
    last_modified: str | None


class FilesResponse(BaseModel):
    files: list[FileEntry]


class PresignedUrlResponse(BaseModel):
    url: str
    expires_in: int  # seconds


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
