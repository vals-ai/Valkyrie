"""API request and response types for the tracker service.

Most types are defined in tracker_shared and re-exported here for backward
compatibility. This module only adds tracker-specific extensions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

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
    FinalViewResponse,
    HarnessConfig,
    Order,
    RetrieveResultsResponse,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    StartBenchmarkErrorResponse,
    StartBenchmarkResponse,
    StatusResponse,
    StopBenchmarkResponse,
    VerifyTaskIdsResponse,
)
from tracker_shared.types import StartBenchmarkRequest as _StartBenchmarkRequestBase

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


__all__ = [
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
