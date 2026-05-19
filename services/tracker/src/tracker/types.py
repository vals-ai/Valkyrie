"""API request and response types for the tracker service.

Most types are defined in tracker_shared and re-exported here for backward
compatibility. This module extends types that need access to the SQLModel
FinalEvaluation (database) class.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pydantic import BaseModel

from tracker_shared.types import (
    AWSCredentials,
    AverageTaskBreakdown,
    BenchmarkDetails,
    BenchmarkTableRow,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FetchBenchmarkTasksRequest,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    HarnessConfig,
    Order,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    StartBenchmarkErrorResponse,
    StartBenchmarkResponse,
    StatusResponse,
    StopBenchmarkResponse,
    VerifyTaskIdsResponse,
)
from tracker_shared.types import StartBenchmarkRequest as _StartBenchmarkRequestBase

from tracker.database.models import BenchmarkArguments, BenchmarkStatus, FinalEvaluation

if TYPE_CHECKING:
    from benchmark_service.client import BenchmarkServiceClient


class StartBenchmarkRequest(_StartBenchmarkRequestBase):
    """Tracker-specific extension that adds the benchmark_service property."""

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
