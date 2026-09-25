"""Version-one task commands; no ORM or service configuration imports."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue

from tracker.executor_api.v1.schemas import DispatchRequest

Duration = Annotated[float, Field(ge=0, allow_inf_nan=False)]


class TaskAttemptRequest(DispatchRequest):
    command_id: UUID
    expected_started_at: AwareDatetime


class TaskMutation(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class BuildTask(TaskMutation):
    operation: Literal["build"] = "build"


class RunTask(TaskMutation):
    operation: Literal["run"] = "run"


class EvaluateTask(TaskMutation):
    operation: Literal["evaluate"] = "evaluate"
    sandbox_build_duration: Duration | None = None
    agent_run_duration: Duration | None = None


class SaveCheckpoint(TaskMutation):
    operation: Literal["checkpoint"] = "checkpoint"
    checkpoint: dict[str, JsonValue]


class CompleteTask(TaskMutation):
    operation: Literal["complete"] = "complete"
    result: dict[str, JsonValue]
    instance_id: str | None = None
    exit_reason: Literal["TIMEOUT", "OS_KILLED"] | None = None
    evaluation_run_duration: Duration | None = None
    sandbox_run_duration: Duration | None = None


class TaskError(TaskMutation):
    error_message: str = Field(min_length=1, max_length=65536)
    producer: str = Field(min_length=1)
    operation_name: str = Field(min_length=1)
    error_type: str = Field(min_length=1)
    cause_code: str | None = None


class FailTask(TaskError):
    operation: Literal["fail"] = "fail"


class RetryTask(TaskError):
    operation: Literal["retry"] = "retry"
    failed_attempt_number: int = Field(ge=1)


class PendingTask(TaskMutation):
    operation: Literal["pending"] = "pending"


class StopTask(TaskMutation):
    operation: Literal["stop"] = "stop"


Mutation = Annotated[
    BuildTask | RunTask | EvaluateTask | SaveCheckpoint | CompleteTask | FailTask | RetryTask | PendingTask | StopTask,
    Field(discriminator="operation"),
]


class TaskWriteRequest(TaskAttemptRequest):
    expected_revision: int = Field(ge=0)
    mutation: Mutation


class TaskWriteResponse(BaseModel):
    command_id: UUID
    task_id: UUID
    revision: int
