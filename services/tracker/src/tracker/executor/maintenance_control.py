"""Private database control for stage-wide deployment maintenance."""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlmodel import Session, col, select

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Task,
    TaskStatus,
)
from tracker.executor.release_control import ReleaseControlError, get_executor_admission

_ACTIVE_BENCHMARK_STATUSES = (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)
_ACTIVE_DISPATCH_STATUSES = (ExecutorDispatchStatus.QUEUED, ExecutorDispatchStatus.RUNNING)
_ACTIVE_TASK_STATUSES = (
    TaskStatus.PENDING,
    TaskStatus.BUILDING,
    TaskStatus.IN_PROGRESS,
    TaskStatus.EVALUATING,
)


class MaintenanceOwnershipError(ReleaseControlError):
    """Raised when a deployment does not own the active maintenance fence."""


@dataclass(frozen=True)
class MaintenanceStopSummary:
    benchmarks: int
    tasks: int
    dispatches: int


def begin_maintenance(
    session: Session,
    *,
    target_sha: str,
) -> MaintenanceStopSummary:
    """Close admission and terminalize every active executor workload."""
    if not target_sha:
        raise ValueError("Maintenance target SHA is required")

    admission = get_executor_admission(session, for_update=True)
    if admission.maintenance_target_sha not in (None, target_sha):
        raise MaintenanceOwnershipError("Executor maintenance is owned by another deployment")

    admission.maintenance_target_sha = target_sha
    admission.updated_at = datetime.now(UTC)
    session.add(admission)

    benchmarks = session.exec(
        select(Benchmark)
        .where(col(Benchmark.status).in_(_ACTIVE_BENCHMARK_STATUSES))
        .order_by(col(Benchmark.id))
        .with_for_update()
    ).all()
    for benchmark in benchmarks:
        benchmark.status = BenchmarkStatus.STOPPED
        session.add(benchmark)

    tasks = list(
        session.exec(
            select(Task).where(col(Task.status).in_(_ACTIVE_TASK_STATUSES)).order_by(col(Task.id)).with_for_update()
        ).all()
    )
    for task in tasks:
        task.status = TaskStatus.STOPPED
        session.add(task)

    dispatches = list(
        session.exec(
            select(ExecutorDispatch)
            .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
            .order_by(col(ExecutorDispatch.id))
            .with_for_update()
        ).all()
    )
    finished_at = datetime.now(UTC)
    for dispatch in dispatches:
        dispatch.status = ExecutorDispatchStatus.FAILED
        dispatch.finished_at = finished_at
        session.add(dispatch)

    session.flush()
    return MaintenanceStopSummary(
        benchmarks=len(benchmarks),
        tasks=len(tasks),
        dispatches=len(dispatches),
    )


def finish_maintenance(
    session: Session,
    *,
    target_sha: str,
) -> None:
    """Open admission only for the deployment that owns the fence."""
    admission = get_executor_admission(session, for_update=True)
    if admission.maintenance_target_sha is None:
        return
    if admission.maintenance_target_sha != target_sha:
        raise MaintenanceOwnershipError("Deployment does not own executor maintenance")

    admission.maintenance_target_sha = None
    admission.updated_at = datetime.now(UTC)
    session.add(admission)
    session.flush()
