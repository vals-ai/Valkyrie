"""Internal types for the tracker service."""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from tracker.benchmark_service import BenchmarkService
from tracker.config import BENCHMARK_SERVICE_URL


class AgentContractRequest(BaseModel):
    """Contract that defines how to upload, install, and run an agent."""

    name: str
    """Name of the agent."""

    artifacts: list[str] = []
    """Paths to artifacts."""

    install_cmd: str
    """Command to install the agent."""

    run_cmd: str
    """Command to run the agent."""

    env: dict[str, str] = {}
    """Environment variables required to run the agent."""


class BenchmarkStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FINISHED = "finished"
    ERROR = "error"


class BenchmarkArguments(BaseModel):
    model_config = {"extra": "forbid"}

    contract: AgentContractRequest
    concurrency: int
    task_ids: list[str] | None = None
    slice_str: str | None = None


class BenchmarkDetails(BaseModel):
    status: BenchmarkStatus
    started_at: datetime
    total_tasks: int
    finished_tasks: int


class StartRunRequest(BaseModel):
    contract: AgentContractRequest
    benchmark_name: str
    concurrency: int = 5
    task_ids: list[str] | None = None
    slice_str: str | None = None

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


class FetchBenchmarkResponse(BaseModel):
    benchmark_name: str
    benchmark_id: UUID
    details: BenchmarkDetails


class FinalEvaluationResponse(BaseModel):
    id: UUID
    benchmark: UUID
    final_score: float
    # NOTE: metadata was reserved by alchemy
    properties: dict[str, Any] = {}


class RetrieveResultsResponse(BaseModel):
    benchmark_name: str
    status: BenchmarkStatus
    benchmark_id: UUID
    benchmark_arguments: BenchmarkArguments
    final_evaluation: FinalEvaluationResponse | None
    evaluation_results: dict[str, dict[str, Any]] | None


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


class VerifyTaskIdsResponse(BaseModel):
    task_ids: list[str]


class StopRunResponse(StatusResponse):
    pass


class ResumeRunResponse(StatusResponse):
    pass
