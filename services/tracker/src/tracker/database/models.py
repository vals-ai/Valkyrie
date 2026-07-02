from datetime import datetime
from enum import Enum
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from pydantic import BaseModel, field_serializer, field_validator
from sqlalchemy import Connection, Dialect, Index, event, text
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
        BenchmarkTableRow,
        FetchBenchmarkMetadataResponse,
        HarnessConfig,
        StartBenchmarkRequest,
    )


DEFAULT_ORG_NAME = "default"


class Org(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    name: str = Field(unique=True, index=True)


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


class DocentReadingStatus(str, Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    DONE = "DONE"


class AgentCausedExitReason(str, Enum):
    """Exit reasons caused by the agent that continue to evaluation"""

    TIMEOUT = "TIMEOUT"
    OS_KILLED = "OS_KILLED"


class RetryMode(str, Enum):
    AUTO = "auto"
    FROM_SCRATCH = "from_scratch"


MAX_OUTPUT_ARTIFACT_BYTES = 50 * 1024 * 1024
MAX_OUTPUT_ARTIFACT_COUNT = 10


def _source_has_glob(source: str) -> bool:
    return any(char in source for char in "*?[")


def _source_glob_root(source: str) -> str:
    glob_indices = [source.find(char) for char in "*?[" if source.find(char) != -1]
    first_glob_index = min(glob_indices)
    root = source[:first_glob_index].rsplit("/", 1)[0]
    return root or "/"


class OutputArtifact(BaseModel):
    path: str
    source: str | None = None

    @field_validator("source")
    @classmethod
    def validate_source(cls, value: str | None) -> str | None:
        if not value:
            return None
        if not value.startswith("/"):
            raise ValueError("output_artifacts source paths must be absolute sandbox paths")

        path = PurePosixPath(value)
        if not path.parts or ".." in path.parts or "." in path.parts:
            raise ValueError("output_artifacts source paths cannot contain empty, '.', or '..' path parts")
        if _source_has_glob(value) and _source_glob_root(value) == "/":
            raise ValueError("output_artifacts glob sources must include a non-root directory prefix")

        return value


OutputArtifactSpec = str | OutputArtifact


class AgentContractRequest(BaseModel):
    name: str
    model: str | None = None
    install_cmd: str = ""
    run_cmd: str = ""
    final_output: str | None = None
    output_artifacts: list[OutputArtifactSpec] = []
    secrets: dict[str, str] = {}
    kwargs: dict[str, str] = {}

    @field_validator("output_artifacts")
    @classmethod
    def validate_output_artifacts(cls, value: list[OutputArtifactSpec]) -> list[OutputArtifactSpec]:
        if len(value) > MAX_OUTPUT_ARTIFACT_COUNT:
            raise ValueError(f"output_artifacts cannot contain more than {MAX_OUTPUT_ARTIFACT_COUNT} entries")

        normalized_artifacts: list[OutputArtifactSpec] = []
        for artifact in value:
            artifact_path = artifact if isinstance(artifact, str) else artifact.path
            path = PurePosixPath(artifact_path)
            if path.is_absolute():
                raise ValueError("output_artifacts paths must be relative paths")
            if not path.parts or ".." in path.parts or "." in path.parts:
                raise ValueError("output_artifacts paths cannot contain empty, '.', or '..' path parts")
            if isinstance(artifact, str):
                normalized_artifacts.append(str(path))
            else:
                normalized_artifacts.append(artifact.model_copy(update={"path": str(path)}))

        return normalized_artifacts


class BenchmarkArguments(BaseModel):
    model_config = {"extra": "forbid"}

    contract: AgentContractRequest
    concurrency: int
    task_ids: list[str] | None = None
    slice_str: str | None = None
    lambda_function: str | None = None
    dataset: str | None = None
    sandbox_provider: str = "daytona"
    sandbox_provider_secret_name: str | None = None


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
    __table_args__ = (
        CheckConstraint(
            "(status != 'FINISHED' AND status != 'ERROR') OR (finished_at IS NOT NULL)",
            name="benchmark_finished_requires_timestamp",
        ),
        Index(
            "ix_benchmark_org_started_at_id",
            "org_id",
            text("started_at DESC"),
            "id",
        ),
    )

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    org_id: UUID = Field(foreign_key="org.id")
    name: str
    started_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))
    finished_at: datetime | None = None
    status: BenchmarkStatus = Field(default=BenchmarkStatus.IN_PROGRESS)
    label: str | None = Field(default=None, index=True)

    error_message: str | None = Field(default=None)
    webhook_secret_name: str | None = Field(default=None)
    webhook_intervals: list[int] | None = Field(default=None, sa_column=Column(JSON))
    custom_benchmark_service: str | None = Field(default=None)
    arguments: BenchmarkArguments = Field(
        sa_column=Column(BenchmarkArgumentsType),
    )
    started_by_id: str | None = Field(default=None)
    started_by_email: str | None = Field(default=None, index=True)
    docent_reading_status: DocentReadingStatus = Field(default=DocentReadingStatus.IDLE)
    docent_reading_url: str | None = Field(default=None)
    final_evaluation: Mapped[FinalEvaluation | None] = Relationship(
        sa_relationship_kwargs={"foreign_keys": "[FinalEvaluation.benchmark]"}
    )

    def fetch_evaluation_results(self, session: Session) -> dict[str, dict[str, Any]]:
        from tracker.utils import fetch_evaluation_results

        return fetch_evaluation_results(self.id, session, self.org_id)

    def fetch_tasks_with_errors(self, session: Session) -> dict[str, str] | None:
        error_rows = session.exec(
            select(Task.task_id, ErrorResult.error_message)
            .outerjoin(ErrorResult, col(ErrorResult.task) == col(Task.id))
            .where(Task.benchmark == self.id)
            .where(Task.org_id == self.org_id)
            .where(Task.status == TaskStatus.ERROR)
            .order_by(col(Task.id), col(ErrorResult.created_at).desc())
        ).all()

        if not error_rows:
            return None

        errors_by_task_id: dict[str, str] = {}
        for task_id, error_message in error_rows:
            errors_by_task_id.setdefault(task_id, error_message or "No error message was provided")

        return errors_by_task_id

    def start_benchmark_request(
        self, harness_config: "HarnessConfig", service_headers: dict[str, str] | None = None
    ) -> "StartBenchmarkRequest":
        from tracker.types import StartBenchmarkRequest

        # TODO: Remove this fallback after legacy benchmark rows have been migrated for a few weeks.
        if self.arguments.sandbox_provider_secret_name:
            harness_config = harness_config.model_copy(
                update={"sandbox_provider_secret_name": self.arguments.sandbox_provider_secret_name}
            )

        return StartBenchmarkRequest(
            contract=self.arguments.contract,
            benchmark_name=self.name,
            concurrency=self.arguments.concurrency,
            task_ids=self.arguments.task_ids,
            slice_str=self.arguments.slice_str,
            lambda_function=self.arguments.lambda_function,
            dataset=self.arguments.dataset,
            harness_config=harness_config,
            sandbox_provider=self.arguments.sandbox_provider,
            custom_benchmark_service=self.custom_benchmark_service,
            webhook_secret_name=self.webhook_secret_name,
            webhook_intervals=self.webhook_intervals,
            service_headers=service_headers or {},
        )

    def benchmark_service(self, service_headers: dict[str, str] | None = None) -> "BenchmarkServiceClient":
        from tracker.config import create_benchmark_service_url
        from tracker.utils import create_benchmark_service_client

        url = self.custom_benchmark_service or create_benchmark_service_url(self.name)
        return create_benchmark_service_client(url=url, service_headers=service_headers)

    @property
    def benchmark_metadata(self) -> "FetchBenchmarkMetadataResponse":
        from tracker.types import FetchBenchmarkMetadataResponse

        return FetchBenchmarkMetadataResponse(
            benchmark_id=self.id,
            benchmark_name=self.name,
            benchmark_arguments=self.arguments,
            started_by_email=self.started_by_email,
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
        finished_tasks = (
            task_state_counts.get(TaskStatus.FINISHED, 0)
            + task_state_counts.get(TaskStatus.ERROR, 0)
            + task_state_counts.get(TaskStatus.STOPPED, 0)
        )

        return BenchmarkTableRow(
            id=self.id,
            name=self.name,
            agent_name=self.arguments.contract.name,
            model=self.arguments.contract.model,
            dataset=self.arguments.dataset or "default",
            started_by_email=self.started_by_email,
            started_at=self.started_at,
            finished_at=self.finished_at,
            status=self.status,
            total_tasks=total_tasks,
            finished_tasks=finished_tasks,
            task_state_counts={k.value: v for k, v in task_state_counts.items()},
            final_score=(self.final_evaluation.final_score if self.final_evaluation else None),
            label=self.label,
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
    finished_at: datetime | None = None
    eval_resume_state: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON, nullable=True))
    benchmark: UUID = Field(foreign_key="benchmark.id")
    task_breakdown: UUID | None = Field(default=None, foreign_key="taskbreakdown.id")


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


class TaskBreakdown(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True, exclude=True)
    sandbox_build_duration: float | None = Field(default=None)
    agent_run_duration: float | None = Field(default=None)
    evaluation_run_duration: float | None = Field(default=None)
    sandbox_run_duration: float | None = Field(default=None)


class ResultBase(SQLModel):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    org_id: UUID = Field(foreign_key="org.id")
    task: UUID = Field(foreign_key="task.id")
    created_at: datetime = Field(default_factory=lambda: datetime.now(ZoneInfo("UTC")))


class EvaluationResult(ResultBase, table=True):
    instance_id: str | None = Field(default=None, unique=True)
    agent_caused_exit_reason: AgentCausedExitReason | None = Field(default=None)
    result: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))


class ErrorResult(ResultBase, table=True):
    error_message: str = Field(nullable=False)
