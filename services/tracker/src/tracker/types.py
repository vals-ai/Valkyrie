"""API request and response types for the tracker service.

Most types are defined in tracker_shared and re-exported here for backward
compatibility. This module extends types that need access to the SQLModel
FinalEvaluation (database) class.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pydantic import BaseModel, Field

from tracker_shared.models import AgentContractRequest, DocentReadingStatus, TaskStatus
from tracker_shared.types import (
    AverageTaskBreakdown,
    BenchmarkTableRow,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FetchBenchmarkTasksRequest,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    Order,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    StartBenchmarkErrorResponse,
    StartBenchmarkResponse,
    StatusResponse,
    StopBenchmarkResponse,
    VerifyTaskIdsResponse,
)

from tracker.database.models import BenchmarkArguments, BenchmarkStatus, FinalEvaluation

if TYPE_CHECKING:
    from benchmark_service.client import BenchmarkServiceClient


class BenchmarkDetails(BaseModel):
    status: BenchmarkStatus
    started_at: datetime
    total_tasks: int
    finished_tasks: int
    task_breakdown: dict[TaskStatus, int]
    docent_reading_status: DocentReadingStatus
    docent_reading_url: str | None = None


class AWSCredentials(BaseModel, frozen=True):
    aws_access_key_id: str
    aws_secret_access_key: str = Field(repr=False)
    aws_default_region: str
    aws_session_token: str | None = Field(default=None, repr=False)


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
    service_headers: dict[str, str] = Field(default_factory=dict, repr=False)
    webhook_secret_name: str | None = None
    webhook_intervals: list[int] | None = None

    @property
    def benchmark_service(self) -> BenchmarkServiceClient:
        from tracker.config import create_benchmark_service_url
        from tracker.utils import create_benchmark_service_client

        benchmark_service_url = self.custom_benchmark_service or create_benchmark_service_url(self.benchmark_name)
        return create_benchmark_service_client(
            url=benchmark_service_url,
            daytona_secret_name=self.harness_config.daytona_secret_name,
            aws=self.harness_config.aws,
            service_headers=self.service_headers,
        )


class FinalViewResponse(BaseModel):
    """Tracker-specific version that accepts the SQLModel FinalEvaluation."""

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


RetrieveResultsResponse = FinalViewResponse | S3UploadResultsResponse


class AnalyzeBenchmarkRequest(BaseModel):
    no_cache: bool = False
    lambda_function: str | None = None


__all__ = [
    "AnalyzeBenchmarkRequest",
    "AWSCredentials",
    "AverageTaskBreakdown",
    "BenchmarkDetails",
    "BenchmarkTableRow",
    "FetchBenchmarkMetadataResponse",
    "FetchBenchmarkResponse",
    "FetchBenchmarkTasksRequest",
    "FetchBenchmarksRequest",
    "FetchBenchmarksResponse",
    "FinalViewResponse",
    "HarnessConfig",
    "Order",
    "RetrieveResultsResponse",
    "RetryOrResumeBenchmarkResponse",
    "S3UploadResultsResponse",
    "StartBenchmarkErrorResponse",
    "StartBenchmarkRequest",
    "StartBenchmarkResponse",
    "StatusResponse",
    "StopBenchmarkResponse",
    "VerifyTaskIdsResponse",
]
