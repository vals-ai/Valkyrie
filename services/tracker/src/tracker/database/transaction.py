"""Session and repository composition for one tracker transaction."""

from collections.abc import Callable
from functools import cached_property
from types import TracebackType

from sqlmodel import Session

from tracker.database.repositories.benchmark import BenchmarkRepository
from tracker.database.repositories.executor_control import ExecutorControlRepository
from tracker.database.repositories.org import OrgRepository
from tracker.database.repositories.reporting import ReportingRepository
from tracker.database.repositories.run_control import RunControlRepository
from tracker.database.repositories.task import TaskRepository
from tracker.database.repositories.task_execution import TaskExecutionRepository


class TrackerTransaction:
    """Compose repositories around one caller-owned database session.

    Repositories never commit. A transaction created with ``open`` owns and
    closes its fresh session; one created with ``from_session`` never closes
    the supplied session.
    """

    def __init__(self, session: Session, *, owns_session: bool) -> None:
        self.session = session
        self._owns_session = owns_session

    @cached_property
    def benchmarks(self) -> BenchmarkRepository:
        return BenchmarkRepository(self.session)

    @cached_property
    def executor_control(self) -> ExecutorControlRepository:
        return ExecutorControlRepository(self.session)

    @cached_property
    def organizations(self) -> OrgRepository:
        return OrgRepository(self.session)

    @cached_property
    def reporting(self) -> ReportingRepository:
        return ReportingRepository(self.session)

    @cached_property
    def tasks(self) -> TaskRepository:
        return TaskRepository(self.session)

    @cached_property
    def task_execution(self) -> TaskExecutionRepository:
        return TaskExecutionRepository(self.session)

    @cached_property
    def run_control(self) -> RunControlRepository:
        return RunControlRepository(self.session, self.benchmarks, self.tasks)

    @classmethod
    def from_session(cls, session: Session) -> "TrackerTransaction":
        """Compose repositories without taking ownership of a supplied session."""
        return cls(session, owns_session=False)

    @classmethod
    def open(cls, session_factory: Callable[[], Session]) -> "TrackerTransaction":
        """Create a transaction that owns the session returned by ``session_factory``."""
        return cls(session_factory(), owns_session=True)

    def commit(self) -> None:
        """Commit the caller-selected transaction phase."""
        self.session.commit()

    def rollback(self) -> None:
        """Roll back the current transaction phase."""
        self.session.rollback()

    def __enter__(self) -> "TrackerTransaction":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is not None:
            self.rollback()
        if self._owns_session:
            self.session.close()
