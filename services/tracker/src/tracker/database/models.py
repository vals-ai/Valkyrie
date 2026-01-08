from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import Connection, ScalarResult, event
from sqlalchemy.orm import Mapper
from sqlmodel import JSON, CheckConstraint, Column, Field, Session, SQLModel, col, select
from tracker.database.utils import has_field_changed


class BenchmarkStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    FINISHED = "finished"


class TaskStatus(str, Enum):
    STARTING = "starting"
    IN_PROGRESS = "in_progress"
    EVALUATING = "evaluating"
    FINISHED = "finished"


class FinalEvaluation(SQLModel, table=True):
    id: UUID | None = Field(default_factory=uuid4, primary_key=True)
    benchmark_id: UUID = Field(foreign_key="benchmark.id")
    final_score: float = Field(nullable=False)
    resolved_tasks: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    unresolved_tasks: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))

    def fetch_evaluation_results(self, session: Session) -> ScalarResult["EvaluationResult"]:
        """Select all evaluation results for a given benchmark"""
        statement = (
            select(EvaluationResult)
            .join(Task, col(EvaluationResult.task_id) == col(Task.id))
            .where(col(Task.benchmark_id) == self.benchmark_id)
        )
        return session.exec(statement)


class Benchmark(SQLModel, table=True):
    __table_args__: tuple[CheckConstraint, ...] = (
        CheckConstraint(
            "(status != 'FINISHED') OR (finished_at IS NOT NULL)",
            name="benchmark_finished_requires_timestamp",
        ),
    )

    id: UUID | None = Field(default_factory=uuid4, primary_key=True)
    name: str
    started_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    finished_at: datetime | None = None
    status: BenchmarkStatus = Field(default=BenchmarkStatus.IN_PROGRESS)


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
    finished_states = [BenchmarkStatus.FINISHED]

    # Check that the status has actually changed between the current and previous state
    status_changed = has_field_changed(target, "status")

    # If the status has changed and the new status is in a finished state, set the finished_at timestamp
    if status_changed and target.status in finished_states:
        target.finished_at = datetime.now(ZoneInfo("UTC"))


class Task(SQLModel, table=True):
    __table_args__: tuple[CheckConstraint, ...] = (
        CheckConstraint(
            "(status != 'FINISHED') OR (finished_at IS NOT NULL)",
            name="task_finished_requires_timestamp",
        ),
    )

    id: UUID | None = Field(default_factory=uuid4, primary_key=True)
    task_id: str = Field(unique=True)
    status: TaskStatus = Field(default=TaskStatus.STARTING)
    started_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    finished_at: datetime | None = None
    benchmark_id: UUID = Field(foreign_key="benchmark.id")


@event.listens_for(Task, "before_insert")
@event.listens_for(Task, "before_update")
def set_finished_at_when_task_finished(_mapper: Mapper[Task], _connection: Connection, target: Task):
    """
    Serves the same purpose as @set_finished_at_when_benchmark_finished, but for the task model.
    """

    # Task statuses we want the finished_at timestamp to be set when updated to
    finished_states = [TaskStatus.FINISHED]

    # Check that the status has actually changed between the current and previous state
    status_changed = has_field_changed(target, "status")

    # If the status has changed and the new status is in a finished state, set the finished_at timestamp
    if status_changed and target.status in finished_states:
        target.finished_at = datetime.now(ZoneInfo("UTC"))


class EvaluationResult(SQLModel, table=True):
    id: UUID | None = Field(default_factory=uuid4, primary_key=True)
    task_id: UUID = Field(foreign_key="task.id")
    instance_id: str = Field(unique=True)
    result: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
