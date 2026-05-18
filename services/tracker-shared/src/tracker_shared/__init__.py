"""Shared types, enums, and exceptions used by the Valkyrie CLI and tracker service.

This lightweight package avoids pulling in heavy backend dependencies
(SQLAlchemy, FastAPI, Daytona, logfire, boto3, etc.) so the CLI starts fast.
"""

from tracker_shared.exceptions import S3Error, TrackerServiceError
from tracker_shared.models import (
    AgentContractRequest,
    BenchmarkArguments,
    BenchmarkStatus,
    FinalEvaluation,
    RetryMode,
    TaskStatus,
)
from tracker_shared.types import (
    AWSCredentials,
    AverageTaskBreakdown,
    BenchmarkDetails,
    BenchmarkTableRow,
    FetchBenchmarkMetadataResponse,
    FetchBenchmarkResponse,
    FetchBenchmarksRequest,
    FetchBenchmarksResponse,
    FetchBenchmarkTasksRequest,
    FinalViewResponse,
    HarnessConfig,
    Order,
    RetrieveResultsResponse,
    RetryOrResumeBenchmarkResponse,
    S3UploadResultsResponse,
    StartBenchmarkErrorResponse,
    StartBenchmarkRequest,
    StartBenchmarkResponse,
    StatusResponse,
    StopBenchmarkResponse,
    VerifyTaskIdsResponse,
)

__all__ = [
    # Exceptions
    "S3Error",
    "TrackerServiceError",
    # Enums
    "BenchmarkStatus",
    "Order",
    "RetryMode",
    "TaskStatus",
    # Models from database
    "AgentContractRequest",
    "BenchmarkArguments",
    "FinalEvaluation",
    # API types
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
