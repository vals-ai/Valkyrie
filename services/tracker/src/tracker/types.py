"""API request and response types for the tracker service."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pydantic import BaseModel

from tracker.config import BENCHMARK_SERVICE_URL
from tracker.database.models import (
    AgentContractRequest,
    BenchmarkArguments,
    BenchmarkStatus,
    FinalEvaluation,
    TaskStatus,
)

if TYPE_CHECKING:
    from tracker.benchmark_service import BenchmarkService


class BenchmarkDetails(BaseModel):
    status: BenchmarkStatus
    started_at: datetime
    total_tasks: int
    finished_tasks: int
    task_breakdown: dict[TaskStatus, int]


class StartRunRequest(BaseModel):
    contract: AgentContractRequest
    benchmark_name: str
    concurrency: int = 5
    task_ids: list[str] | None = None
    slice_str: str | None = None

    @property
    def benchmark_service(self) -> "BenchmarkService":
        from tracker.benchmark_service import BenchmarkService

        return BenchmarkService(name=self.benchmark_name, url=BENCHMARK_SERVICE_URL)


class StartRunErrorResponse(BaseModel):
    benchmark_id: UUID
    error_message: str


class StartRunResponse(BaseModel):
    benchmark_name: str
    agent_name: str
    benchmark_id: UUID
    concurrency: int
    started_at: datetime
    task_count: int


class FetchBenchmarkResponse(BaseModel):
    benchmark_name: str
    benchmark_id: UUID
    details: BenchmarkDetails


class RetrieveResultsResponse(BaseModel):
    benchmark_name: str
    status: BenchmarkStatus
    error_message: str | None
    benchmark_id: UUID
    benchmark_arguments: BenchmarkArguments
    tasks_stopped: int | None
    final_evaluation: FinalEvaluation | None
    evaluation_results: dict[str, dict[str, Any]] | None
    task_errors: dict[str, str] | None


class FinalScoreResponse(BaseModel):
    tasks_evaluated: list[str]
    final_score: float
    metadata: dict[str, Any]


class StatusResponse(BaseModel):
    status: str


class SetupTaskResponse(StatusResponse):
    pass


class HealthCheckResponse(StatusResponse):
    pass


class RetrieveTaskResponse(BaseModel):
    docker_image: str
    problem_statement: str
    request_setup: bool
    cwd: str


class VerifyTaskIdsResponse(BaseModel):
    task_ids: list[str]


class StopRunResponse(StatusResponse):
    pass


class ResumeRunResponse(StatusResponse):
    pass


class Order(str, Enum):
    ASC = "asc"
    DESC = "desc"


class FetchBenchmarksRequest(BaseModel):
    agent_name: str | None = None
    benchmark_name: str | None = None
    status: BenchmarkStatus | None = None
    order_by: Order = Order.DESC  # Order is based off the time the benchmark was started at

    # Pagination
    limit: int = 5
    offset: int = 0


class BenchmarkTableRow(BaseModel):
    id: UUID
    name: str
    agent_name: str
    started_at: datetime
    status: BenchmarkStatus
    total_tasks: int
    finished_tasks: int


class FetchBenchmarksResponse(BaseModel):
    benchmarks: list[BenchmarkTableRow]
    total_count: int
