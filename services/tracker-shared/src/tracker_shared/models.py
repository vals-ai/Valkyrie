"""Shared models and enums — the canonical source of truth.

Both the CLI and the tracker service import from here. The tracker service
re-exports these from tracker.database.models for backward compatibility
and extends some types with SQLModel/database-specific behaviour.
"""

from __future__ import annotations

from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_serializer


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    BUILDING = "BUILDING"
    IN_PROGRESS = "IN_PROGRESS"
    EVALUATING = "EVALUATING"
    STOPPED = "STOPPED"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


class BenchmarkStatus(str, Enum):
    IN_PROGRESS = "IN_PROGRESS"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FINISHED = "FINISHED"
    ERROR = "ERROR"


class RetryMode(str, Enum):
    AUTO = "auto"
    FROM_SCRATCH = "from_scratch"


class AgentContractRequest(BaseModel):
    name: str
    model: str | None = None
    install_cmd: str
    run_cmd: str
    final_output: str | None = None
    secrets: dict[str, str] = {}


class BenchmarkArguments(BaseModel):
    model_config = {"extra": "forbid"}

    contract: AgentContractRequest
    concurrency: int
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset: str | None = None


class FinalEvaluation(BaseModel):
    """Pydantic model for API serialisation/deserialisation.

    The tracker service defines a separate SQLModel FinalEvaluation table
    for database operations; this Pydantic version is the canonical shape
    for API responses consumed by both the CLI and the tracker's response types.
    """

    id: UUID
    org_id: UUID
    benchmark: UUID
    final_score: float
    properties: dict[str, Any] = Field(default_factory=dict)

    @field_serializer("id", "benchmark")
    def serialize_uuid(self, value: UUID | str) -> str:
        return str(value)
