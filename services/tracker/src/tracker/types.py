"""Internal types for the tracker service."""

from typing import Any

from pydantic import BaseModel

# Re-export shared types for convenience
from agentic_harness.types import (
    BenchmarkArguments,
    BenchmarkDetails,
    BenchmarkStatus,
    FetchBenchmarkResponse,
    FinalEvaluationResponse,
    RetrieveResultsResponse,
    StartRunErrorResponse,
    StartRunRequest,
    StartRunResponse,
)

__all__ = [
    # Tracker API types
    "BenchmarkArguments",
    "BenchmarkDetails",
    "BenchmarkStatus",
    "FetchBenchmarkResponse",
    "FinalEvaluationResponse",
    "RetrieveResultsResponse",
    "StartRunErrorResponse",
    "StartRunRequest",
    "StartRunResponse",
    # SWEBench Service types
    "FinalScoreResponse",
    "StatusResponse",
    "SetupTaskResponse",
    "HealthCheckResponse",
    "RetrieveTaskResponse",
    "VerifyTaskIdsResponse",
]


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
