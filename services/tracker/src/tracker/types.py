"""API request and response types for the tracker service."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from benchmark_service.client import BenchmarkServiceClient
from pydantic import BaseModel

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


class AWSCredentials(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_default_region: str


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
    webhook_url: str | None = None
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
    benchmark_name: str
    status: BenchmarkStatus
    error_message: str | None
    benchmark_id: UUID
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
    agent_name: str | None = None
    benchmark_name: str | None = None
    model: str | None = None
    status: BenchmarkStatus | None = None
    order_by: Order = Order.DESC  # Order is based off the time the benchmark was started at

    # Pagination
    limit: int = 5
    offset: int = 0


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


class FetchBenchmarksResponse(BaseModel):
    benchmarks: list[BenchmarkTableRow]
    total_count: int


class FetchBenchmarkMetadataResponse(BaseModel):
    benchmark_id: UUID
    benchmark_name: str
    benchmark_arguments: BenchmarkArguments
