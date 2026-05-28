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


DEFAULT_ORG_NAME = "default"


class Org(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    name: str = Field(unique=True, index=True)


class User(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    org_id: UUID = Field(foreign_key="org.id", index=True)
    email: str = Field(index=True)
    descope_user_id: str = Field(unique=True, index=True)
    created_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))


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


class AgentCausedExitReason(str, Enum):
    """Exit reasons caused by the agent that continue to evaluation"""

    TIMEOUT = "TIMEOUT"
    OS_KILLED = "OS_KILLED"


class AgentContractRequest(BaseModel):
    name: str
    model: str | None = None
    install_cmd: str
    run_cmd: str
    final_output: str | None = None
    secrets: dict[str, str] = {}
    kwargs: dict[str, str] = {}


class BenchmarkArguments(BaseModel):
    model_config = {"extra": "forbid"}

    contract: AgentContractRequest
    concurrency: int
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset: str | None = None


class FinalEvaluation(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    org_id: UUID = Field(foreign_key="org.id")
    benchmark: UUID = Field(foreign_key="benchmark.id")
    final_score: float = Field(nullable=False)
    # NOTE: metadata was reserved by alchemy
    properties: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))

    @field_serializer("id", "benchmark")
    def serialize_uuid(self, value: UUID | str) -> str:
        return str(value)

    def fetch_evaluation_results(self, session: Session) -> dict[str, dict[str, Any]]:
        from tracker.utils import fetch_evaluation_results

        return fetch_evaluation_results(self.benchmark, session, self.org_id)


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
    org_id: UUID = Field(foreign_key="org.id")
    name: str
    started_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    finished_at: datetime | None = None
    status: BenchmarkStatus = Field(
        default=BenchmarkStatus.IN_PROGRESS
    )  # TODO: Automatically set to finished when all tasks are in a finished state or error state
    run_by_id: UUID | None = Field(default=None, foreign_key="user.id", index=True)

    error_message: str | None = Field(default=None)
    webhook_secret_name: str | None = Field(default=None)
    webhook_intervals: list[int] | None = Field(default=None, sa_column=Column(JSON))
    custom_benchmark_service: str | None = Field(default=None)
    arguments: BenchmarkArguments = Field(
        sa_column=Column(BenchmarkArgumentsType),
    )
    final_evaluation: Mapped[FinalEvaluation | None] = Relationship(
        sa_relationship_kwargs={"foreign_keys": "[FinalEvaluation.benchmark]"}
    )

    def fetch_evaluation_results(self, session: Session) -> dict[str, dict[str, Any]]:
        from tracker.utils import fetch_evaluation_results

        return fetch_evaluation_results(self.id, session, self.org_id)

    def fetch_tasks_with_errors(self, session: Session) -> dict[str, str] | None:
        statement = (
            select(Task.task_id, Task.error_message)
            .where(Task.benchmark == self.id)
            .where(Task.org_id == self.org_id)
            .where(Task.status == TaskStatus.ERROR)
        )
        tasks = session.exec(statement).all()

        if not tasks:
            return None

        return {task_id: (error_message or "No error message was provided") for task_id, error_message in tasks}

    def start_benchmark_request(
        self, harness_config: "HarnessConfig", service_headers: dict[str, str] | None = None
    ) -> "StartBenchmarkRequest":
        from tracker.types import StartBenchmarkRequest

        return StartBenchmarkRequest(
            contract=self.arguments.contract,
            benchmark_name=self.name,
            concurrency=self.arguments.concurrency,
            task_ids=self.arguments.task_ids,
            slice_str=self.arguments.slice_str,
            lambda_function=self.arguments.lambda_function,
            dataset=self.arguments.dataset,
            harness_config=harness_config,
            custom_benchmark_service=self.custom_benchmark_service,
            webhook_secret_name=self.webhook_secret_name,
            webhook_intervals=self.webhook_intervals,
            service_headers=service_headers or {},
        )

    def benchmark_service(
        self, daytona_secret_name: str, aws: "AWSCredentials", service_headers: dict[str, str] | None = None
    ) -> "BenchmarkServiceClient":
        from tracker.config import create_benchmark_service_url
        from tracker.utils import create_benchmark_service_client

        url = self.custom_benchmark_service or create_benchmark_service_url(self.name)
        return create_benchmark_service_client(
            url=url, daytona_secret_name=daytona_secret_name, aws=aws, service_headers=service_headers
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

        task_state_counts = self.fetch_task_state_counts(session)
        total_tasks = sum(task_state_counts.values())
        finished_tasks = task_state_counts.get(TaskStatus.FINISHED, 0) + task_state_counts.get(TaskStatus.ERROR, 0)

        run_by_email: str | None = None
        if self.run_by_id is not None:
            user_obj = session.get(User, self.run_by_id)
            run_by_email = user_obj.email if user_obj else None

        return BenchmarkTableRow(
            id=self.id,
            name=self.name,
            agent_name=self.arguments.contract.name,
            model=self.arguments.contract.model,
            started_at=self.started_at,
            finished_at=self.finished_at,
            status=self.status,
            total_tasks=total_tasks,
            finished_tasks=finished_tasks,
            task_state_counts={k.value: v for k, v in task_state_counts.items()},
            run_by_email=run_by_email,
            final_score=(self.final_evaluation.final_score if self.final_evaluation else None),
        )

    def fetch_task_state_counts(self, session: Session) -> dict[TaskStatus, int]:
        """Count this benchmark's tasks grouped by TaskStatus."""
        rows = session.exec(
            select(col(Task.status), func.count(col(Task.task_id)))
            .where(col(Task.benchmark) == self.id)
            .where(col(Task.org_id) == self.org_id)
            .group_by(col(Task.status))
        ).all()
        return {status: count for status, count in rows}

    def fetch_final_score(self, session: Session) -> float | None:
        """Return the FinalEvaluation.final_score for this benchmark, or None if no row exists."""
        row = session.exec(
            select(FinalEvaluation.final_score)
            .where(col(FinalEvaluation.benchmark) == self.id)
            .where(col(FinalEvaluation.org_id) == self.org_id)
        ).first()
        return row if row is not None else None


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
    org_id: UUID = Field(foreign_key="org.id")
    task_id: str
    status: TaskStatus = Field(default=TaskStatus.PENDING)
    started_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    error_message: str | None = Field(default=None)
    finished_at: datetime | None = None
    benchmark: UUID = Field(foreign_key="benchmark.id")

    @computed_field
    @property
    def alias(self) -> str:
        """Unique alias for the current task attempt, used when creating sandboxes.

        Format: {task_id}_{suffix}
        - suffix: hex-encoded microsecond timestamp from started_at, changes on each retry/resume
        """
        suffix = f"{int(self.started_at.timestamp() * 1_000_000):x}"
        return f"{self.task_id}_{suffix}"


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
    org_id: UUID = Field(foreign_key="org.id")
    task: UUID = Field(foreign_key="task.id")
    instance_id: str = Field(unique=True)
    agent_caused_exit_reason: AgentCausedExitReason | None = Field(default=None)
    result: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))


class OrgConfig(SQLModel, table=True):
    __tablename__ = "org_config"  # type: ignore[assignment]

    org_id: UUID = Field(foreign_key="org.id", primary_key=True)
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_default_region: str
    s3_bucket: str
    daytona_secret_name: str
    log_group: str | None = Field(default=None)
    log_retention_policy: str | None = Field(default=None)
    webhook: str | None = Field(default=None)
    benchmark_services: list[dict[str, Any]] = Field(
        default_factory=list,
        sa_column=Column(JSON, nullable=False, server_default="[]"),
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(ZoneInfo("UTC")),
        sa_column_kwargs={
            "onupdate": lambda: datetime.now(ZoneInfo("UTC")),
        },
    )
