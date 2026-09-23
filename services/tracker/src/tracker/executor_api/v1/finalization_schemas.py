"""Version-one finalization requests and snapshots, independent of the ORM."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from tracker.executor_api.v1.schemas import DispatchRequest, RunStatus


class FinalizationResponse(BaseModel):
    benchmark_id: UUID
    current: bool
    status: RunStatus
    snapshot_digest: str | None
    operation: Literal["complete", "fail", "stop"] | None
    evaluation_results: dict[str, dict[str, JsonValue] | None]
    task_errors: dict[str, str]


class CompleteRun(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    operation: Literal["complete"] = "complete"
    final_score: float
    metadata: dict[str, JsonValue] = Field(default_factory=dict)


class FailRun(BaseModel):
    model_config = ConfigDict(extra="forbid")
    operation: Literal["fail"] = "fail"
    error_message: str = Field(min_length=1, max_length=65536)


class StopRun(BaseModel):
    model_config = ConfigDict(extra="forbid")
    operation: Literal["stop"] = "stop"


Finalization = Annotated[CompleteRun | FailRun | StopRun, Field(discriminator="operation")]


class FinalizeRequest(DispatchRequest):
    command_id: UUID
    snapshot_digest: str = Field(pattern="^[0-9a-f]{64}$")
    finalization: Finalization


class FinalizeResponse(BaseModel):
    command_id: UUID
    benchmark_id: UUID
    status: Literal["FINISHED", "ERROR", "STOPPED"]
    final_evaluation_id: UUID | None
