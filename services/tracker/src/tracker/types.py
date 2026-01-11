from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from tracker.benchmark_service import BenchmarkService
from tracker.config import BENCHMARK_SERVICE_URL
from tracker.database.models import BenchmarkArguments, BenchmarkStatus, FinalEvaluation


class StartRunRequest(BaseModel):
    contract_name: str
    benchmark_name: str
    concurrency: int = 5
    task_ids: list[str] | None = None

    @property
    def benchmark_service(self) -> BenchmarkService:
        return BenchmarkService(name=self.benchmark_name, url=BENCHMARK_SERVICE_URL)


class StartRunErrorResponse(BaseModel):
    benchmark_id: UUID
    error_message: str


class StartRunResponse(BaseModel):
    benchmark_name: str
    contract_name: str
    benchmark_id: UUID
    concurrency: int
    started_at: datetime
    task_count: int


class BenchmarkDetails(BaseModel):
    status: BenchmarkStatus
    started_at: datetime
    total_tasks: int
    finished_tasks: int


class FetchBenchmarkResponse(BaseModel):
    benchmark_name: str
    benchmark_id: UUID
    details: BenchmarkDetails


class RetrieveResultsResponse(BaseModel):
    benchmark_name: str
    status: BenchmarkStatus
    benchmark_id: UUID
    benchmark_arguments: BenchmarkArguments
    final_evaluation: FinalEvaluation | None
    evaluation_results: dict[str, dict[str, Any]] | None
