from datetime import datetime
from typing import Any

from pydantic import BaseModel

from tracker.benchmark_service import BenchmarkService
from tracker.config import BENCHMARK_SERVICE_URL


class StartRunRequest(BaseModel):
    contract_name: str
    benchmark_name: str
    concurrency: int = 5
    task_ids: list[str] | None = None

    @property
    def benchmark_service(self) -> BenchmarkService:
        return BenchmarkService(name=self.benchmark_name, url=BENCHMARK_SERVICE_URL)


class StartRunResponse(BaseModel):
    benchmark_name: str
    contract_name: str
    concurrency: int
    started_at: datetime
    finished_at: datetime
    task_ids: list[str]
    final_score: float
    resolved_tasks: list[str]
    unresolved_tasks: list[str]
    evaluation_results: dict[str, dict[str, Any]]
