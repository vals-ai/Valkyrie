from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from pydantic import BaseModel, field_serializer
from sqlalchemy import Connection, Dialect, event
from sqlalchemy.orm import Mapped, Mapper
from sqlmodel import (
    JSON,
    CheckConstraint,
    Column,
    Field,
    Relationship,
    Session,
    SQLModel,
    TypeDecorator,
    UniqueConstraint,
)

from tracker.database.utils import has_field_changed


class BenchmarkStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    FINISHED = "finished"
    ERROR = "error"


class TaskStatus(str, Enum):
    STARTING = "starting"
    IN_PROGRESS = "in_progress"
    EVALUATING = "evaluating"
    FINISHED = "finished"
    ERROR = "error"


class FinalEvaluation(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    benchmark: UUID = Field(foreign_key="benchmark.id")
    final_score: float = Field(nullable=False)
    # NOTE: metadata was reserved by alchemy
    properties: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))

    @field_serializer("id", "benchmark")
    def serialize_uuid(self, value: UUID | str) -> str:
        return str(value)


class BenchmarkArguments(BaseModel):
    model_config = {"extra": "forbid"}

    contract_name: str
    concurrency: int
    task_ids: list[str] | None = None
    slice_str: str | None = None


class BenchmarkArgumentsType(TypeDecorator[BenchmarkArguments]):
    """
    Hook for converting benchmark arguments to an object and back again.
    Allows us to use the type without manually serializing and deserializing.
    NOTE: We do this because the field is not relevant enough to be a separate table and we want it created with the benchmark row

    Related Documentation:
        - https://docs.sqlalchemy.org/en/20/core/custom_types.html#sqlalchemy.types.TypeDecorator
    """

    impl = JSON
    cache_ok = True

    def process_bind_param(self, value: BenchmarkArguments | None, dialect: Dialect) -> dict[str, Any] | None:
        """Runs when we save the value to the database."""
        if value is None:
            return None
        return value.model_dump()

    def process_result_value(self, value: dict[str, Any] | None, dialect: Dialect) -> BenchmarkArguments | None:
        """Runs when we fetch the value from the database."""
        if value is None:
            return None
        return BenchmarkArguments(**value)


class Benchmark(SQLModel, table=True):
    __table_args__: tuple[CheckConstraint, ...] = (
        CheckConstraint(
            "(status != 'FINISHED' AND status != 'ERROR') OR (finished_at IS NOT NULL)",
            name="benchmark_finished_requires_timestamp",
        ),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    name: str
    started_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    finished_at: datetime | None = None
    status: BenchmarkStatus = Field(
        default=BenchmarkStatus.IN_PROGRESS
    )  # TODO: Automatically set to finished when all tasks are in a finished state or error state

    error_message: str | None = Field(default=None)
    arguments: BenchmarkArguments = Field(
        sa_column=Column(BenchmarkArgumentsType),
    )
    final_evaluation: Mapped[FinalEvaluation | None] = Relationship(
        sa_relationship_kwargs={"foreign_keys": "[FinalEvaluation.benchmark]"}
    )

    def fetch_evaluation_results(self, session: Session) -> dict[str, dict[str, Any]]:
        from tracker.utils import fetch_evaluation_results

        return fetch_evaluation_results(self.id, session)


@event.listens_for(Benchmark, "before_insert")
@event.listens_for(Benchmark, "before_update")
def set_finished_at_when_benchmark_finished(_mapper: Mapper[Benchmark], _connection: Connection, target: Benchmark):
    """
    Automatically set the finished_at timestamp when the benchmark is finished.

    Prevents situations where the benchmark can be finished but we never set the finished_at timestamp.

    Related Documentation:
        - https://docs.sqlalchemy.org/en/20/orm/events.html#mapper-events
        - https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.attributes.get_history
    """

    # Benchmark statuses we want the finished_at timestamp to be set when updated to
    finished_states = [BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR]

    # Check that the status has actually changed between the current and previous state
    status_changed = has_field_changed(target, "status")

    # If the status has changed and the new status is in a finished state, set the finished_at timestamp
    if status_changed and target.status in finished_states:
        target.finished_at = datetime.now(ZoneInfo("UTC"))


class Task(SQLModel, table=True):
    __table_args__: tuple[CheckConstraint, UniqueConstraint] = (
        CheckConstraint(
            "(status != 'FINISHED' AND status != 'ERROR') OR (finished_at IS NOT NULL)",
            name="task_finished_requires_timestamp",
        ),
        UniqueConstraint("benchmark", "task_id", name="unique_task_per_benchmark"),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    task_id: str
    status: TaskStatus = Field(default=TaskStatus.STARTING)
    started_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    error_message: str | None = Field(default=None)
    finished_at: datetime | None = None
    benchmark: UUID = Field(foreign_key="benchmark.id")


@event.listens_for(Task, "before_insert")
@event.listens_for(Task, "before_update")
def set_finished_at_when_task_finished(_mapper: Mapper[Task], _connection: Connection, target: Task):
    """
    Serves the same purpose as @set_finished_at_when_benchmark_finished, but for the task model.
    """

    # Task statuses we want the finished_at timestamp to be set when updated to
    finished_states = [TaskStatus.FINISHED, TaskStatus.ERROR]

    # Check that the status has actually changed between the current and previous state
    status_changed = has_field_changed(target, "status")

    # If the status has changed and the new status is in a finished state, set the finished_at timestamp
    if status_changed and target.status in finished_states:
        target.finished_at = datetime.now(ZoneInfo("UTC"))


class EvaluationResult(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    task: UUID = Field(foreign_key="task.id")
    instance_id: str = Field(unique=True)
    result: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
