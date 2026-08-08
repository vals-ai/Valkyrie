"""FastAPI dependencies for request-scoped tracker repositories."""

from typing import Annotated

from fastapi import Depends
from sqlmodel import Session

from tracker.database.repositories import (
    BenchmarkRepository,
    ExecutorControlRepository,
    OrgRepository,
    ReportingRepository,
    RunControlRepository,
    TaskRepository,
)
from tracker.database.session import get_session
from tracker.database.transaction import TrackerTransaction


def get_tracker_transaction(session: Session = Depends(get_session)) -> TrackerTransaction:
    """Provide a multi-repository composition bound to the request session."""
    return TrackerTransaction.from_session(session)


def get_executor_control_repository(session: Session = Depends(get_session)) -> ExecutorControlRepository:
    """Provide an executor-control repository bound to the request session."""
    return ExecutorControlRepository(session)


def get_org_repository(session: Session = Depends(get_session)) -> OrgRepository:
    """Provide an organization repository bound to the request session."""
    return OrgRepository(session)


def get_benchmark_repository(session: Session = Depends(get_session)) -> BenchmarkRepository:
    """Provide a benchmark repository bound to the request session."""
    return BenchmarkRepository(session)


def get_task_repository(session: Session = Depends(get_session)) -> TaskRepository:
    """Provide a task repository bound to the request session."""
    return TaskRepository(session)


def get_reporting_repository(session: Session = Depends(get_session)) -> ReportingRepository:
    """Provide a reporting repository bound to the request session."""
    return ReportingRepository(session)


def get_run_control_repository(
    session: Session = Depends(get_session),
    benchmark_repository: BenchmarkRepository = Depends(get_benchmark_repository),
    task_repository: TaskRepository = Depends(get_task_repository),
) -> RunControlRepository:
    """Provide a run-control repository bound to the request session."""
    return RunControlRepository(session, benchmark_repository, task_repository)


ExecutorControlRepositoryDep = Annotated[ExecutorControlRepository, Depends(get_executor_control_repository)]
OrgRepositoryDep = Annotated[OrgRepository, Depends(get_org_repository)]
BenchmarkRepositoryDep = Annotated[BenchmarkRepository, Depends(get_benchmark_repository)]
TaskRepositoryDep = Annotated[TaskRepository, Depends(get_task_repository)]
ReportingRepositoryDep = Annotated[ReportingRepository, Depends(get_reporting_repository)]
RunControlRepositoryDep = Annotated[RunControlRepository, Depends(get_run_control_repository)]
TrackerTransactionDep = Annotated[TrackerTransaction, Depends(get_tracker_transaction)]
