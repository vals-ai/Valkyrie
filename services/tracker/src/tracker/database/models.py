from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from pydantic import BaseModel, computed_field, field_serializer
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
    col,
    func,
    select,
)

from tracker.database.utils import has_field_changed

if TYPE_CHECKING:
    from benchmark_service.client import BenchmarkServiceClient

    from tracker.types import (
        AWSCredentials,
        BenchmarkTableRow,
        FetchBenchmarkMetadataResponse,
        HarnessConfig,
        StartBenchmarkRequest,
    )


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


class AgentContractRequest(BaseModel):
    name: str
    artifacts: list[str] = []
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


class FinalEvaluation(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    benchmark: UUID = Field(foreign_key="benchmark.id")
    final_score: float = Field(nullable=False)
    # NOTE: metadata was reserved by alchemy
    properties: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))

    @field_serializer("id", "benchmark")
    def serialize_uuid(self, value: UUID | str) -> str:
        return str(value)

    def fetch_evaluation_results(self, session: Session) -> dict[str, dict[str, Any]]:
        from tracker.utils import fetch_evaluation_results

        return fetch_evaluation_results(self.benchmark, session)


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
    custom_benchmark_service: str | None = Field(default=None)
    arguments: BenchmarkArguments = Field(
        sa_column=Column(BenchmarkArgumentsType),
    )
    final_evaluation: Mapped[FinalEvaluation | None] = Relationship(
        sa_relationship_kwargs={"foreign_keys": "[FinalEvaluation.benchmark]"}
    )

    def fetch_evaluation_results(self, session: Session) -> dict[str, dict[str, Any]]:
        from tracker.utils import fetch_evaluation_results

        return fetch_evaluation_results(self.id, session)

    def fetch_tasks_with_errors(self, session: Session) -> dict[str, str] | None:
        statement = (
            select(Task.task_id, Task.error_message)
            .where(Task.benchmark == self.id)
            .where(Task.status == TaskStatus.ERROR)
        )
        tasks = session.exec(statement).all()

        if not tasks:
            return None

        return {task_id: (error_message or "No error message was provided") for task_id, error_message in tasks}

    def start_benchmark_request(self, harness_config: "HarnessConfig") -> "StartBenchmarkRequest":
        from tracker.types import StartBenchmarkRequest

        return StartBenchmarkRequest(
            contract=self.arguments.contract,
            benchmark_name=self.name,
            concurrency=self.arguments.concurrency,
            task_ids=self.arguments.task_ids,
            slice_str=self.arguments.slice_str,
            lambda_function=self.arguments.lambda_function,
            harness_config=harness_config,
            custom_benchmark_service=self.custom_benchmark_service,
        )

    def benchmark_service(self, daytona_secret_name: str, aws: "AWSCredentials") -> "BenchmarkServiceClient":
        from tracker.config import create_benchmark_service_url
        from tracker.utils import create_benchmark_service_client

        return create_benchmark_service_client(
            url=create_benchmark_service_url(self.name), daytona_secret_name=daytona_secret_name, aws=aws
        )

    @property
    def benchmark_metadata(self) -> "FetchBenchmarkMetadataResponse":
        from tracker.types import FetchBenchmarkMetadataResponse

        return FetchBenchmarkMetadataResponse(
            benchmark_id=self.id,
            benchmark_name=self.name,
            benchmark_arguments=self.arguments,
        )

    def create_benchmark_table_row(self, session: Session) -> "BenchmarkTableRow":
        """
        Creates a benchmark table row object used to display the current data from this benchmark row.
        Used in a table like feature amongst other benchmarks rows.

        Args:
            session: Session to use to fetch the total and finished tasks

        Returns:
            BenchmarkTableRow
        """
        from tracker.types import BenchmarkTableRow

        total_tasks: int = session.exec(
            select(func.count(col(Task.task_id))).where(col(Task.benchmark) == self.id)
        ).one()

        finished_tasks: int = session.exec(
            select(func.count(col(Task.task_id)))
            .where(col(Task.benchmark) == self.id)
            .where(col(Task.status).in_([TaskStatus.FINISHED, TaskStatus.ERROR]))
        ).one()

        return BenchmarkTableRow(
            id=self.id,
            name=self.name,
            agent_name=self.arguments.contract.name,
            started_at=self.started_at,
            finished_at=self.finished_at,
            status=self.status,
            total_tasks=total_tasks,
            finished_tasks=finished_tasks,
        )


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
    status: TaskStatus = Field(default=TaskStatus.PENDING)
    started_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    error_message: str | None = Field(default=None)
    finished_at: datetime | None = None
    benchmark: UUID = Field(foreign_key="benchmark.id")

    @computed_field
    @property
    def alias(self) -> str:
        """Unique alias for the task that is used to uniquely identify the same task when creating sandboxes"""
        return f"{self.task_id}_{self.id.hex[:5]}"


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
