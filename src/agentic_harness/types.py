"""API types shared between the CLI client and tracker service."""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from agentic_harness.contract import AgentContract


class BenchmarkStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    FINISHED = "finished"
    ERROR = "error"


class BenchmarkArguments(BaseModel):
    model_config = {"extra": "forbid"}

    contract_name: str
    concurrency: int
    task_ids: list[str] | None = None
    slice_str: str | None = None


class BenchmarkDetails(BaseModel):
    status: BenchmarkStatus
    started_at: datetime
    total_tasks: int
    finished_tasks: int


class StartRunRequest(BaseModel):
    contract: AgentContract
    benchmark_name: str
    concurrency: int = 5
    task_ids: list[str] | None = None
    slice_str: str | None = None


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


class FetchBenchmarkResponse(BaseModel):
    benchmark_name: str
    benchmark_id: UUID
    details: BenchmarkDetails


class FinalEvaluationResponse(BaseModel):
    id: UUID
    benchmark: UUID
    final_score: float
    resolved_tasks: list[str] = []
    unresolved_tasks: list[str] = []


class RetrieveResultsResponse(BaseModel):
    benchmark_name: str
    status: BenchmarkStatus
    benchmark_id: UUID
    benchmark_arguments: BenchmarkArguments
    final_evaluation: FinalEvaluationResponse | None
    evaluation_results: dict[str, dict[str, Any]] | None
