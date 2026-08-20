"""Canonical transaction owners for executor dispatch lifecycle."""

from datetime import datetime
from enum import Enum
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, update
from sqlmodel import Session, col, select

from executor_protocol import MANAGED_EXECUTION_PROTOCOL_VERSION
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Task,
    TaskStatus,
)
from tracker.executor.release_control import (
    ReleaseControlError,
    create_executor_dispatch,
    lock_executor_admission,
    pin_benchmark_to_release,
    resolve_current_execution_release,
    select_active_release,
)

_ACTIVE_DISPATCH_STATUSES = (
    ExecutorDispatchStatus.QUEUED,
    ExecutorDispatchStatus.RUNNING,
)


class EnqueueFailureResolution(str, Enum):
    FAILED = "FAILED"
    DELIVERED = "DELIVERED"
    SUPERSEDED = "SUPERSEDED"


def _require_managed_execution_release(release: ExecutorRelease) -> None:
    if release.protocol_version != MANAGED_EXECUTION_PROTOCOL_VERSION:
        raise ReleaseControlError("Activate an executor release that supports managed runs")


def _require_compatible_release(benchmark: Benchmark, release: ExecutorRelease) -> None:
    if benchmark.aws_managed:
        _require_managed_execution_release(release)


def validate_managed_execution_release(session: Session) -> None:
    """Reject managed work unless the active executor can consume its queue payload."""
    _require_managed_execution_release(select_active_release(session))


def admit_start_dispatch(
    session: Session,
    *,
    benchmark: Benchmark,
    dispatch_id: UUID,
    task_ids: list[str] | None = None,
) -> ExecutorDispatch:
    """Select the active release and persist one start dispatch."""
    with session.no_autoflush:
        release = select_active_release(session, for_update=True)
    _require_compatible_release(benchmark, release)
    session.add(benchmark)
    pin_benchmark_to_release(benchmark, release)
    dispatch = create_executor_dispatch(
        benchmark.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=dispatch_id,
        task_ids=task_ids,
    )
    session.add(dispatch)
    session.flush()
    return dispatch


def admit_recovery_dispatch(
    session: Session,
    *,
    benchmark: Benchmark,
    pre_action_status: BenchmarkStatus,
    dispatch_id: UUID,
    kind: ExecutorDispatchKind,
    task_ids: list[str] | None = None,
) -> ExecutorDispatch:
    """Persist additive in-progress work or replace a terminal execution."""
    with session.no_autoflush:
        lock_executor_admission(session)
        if pre_action_status == BenchmarkStatus.IN_PROGRESS:
            release = resolve_current_execution_release(session, benchmark, for_update=True)
        else:
            release = select_active_release(session, for_update=True)
    _require_compatible_release(benchmark, release)
    if pre_action_status != BenchmarkStatus.IN_PROGRESS:
        benchmark.current_execution_release_id = release.id
        benchmark.finished_at = None
        terminalize_active_dispatches(session, benchmark.id)
    session.add(benchmark)

    dispatch = create_executor_dispatch(
        benchmark.id,
        release,
        kind,
        dispatch_id=dispatch_id,
        task_ids=task_ids,
    )
    session.add(dispatch)
    session.flush()
    return dispatch


def terminalize_active_dispatches(
    session: Session,
    benchmark_id: UUID,
    *,
    except_dispatch_id: UUID | None = None,
) -> None:
    """Fail every active dispatch except an optional selected owner.

    Callers changing benchmark lifecycle state must lock the benchmark row first.
    """
    dispatches = (
        update(ExecutorDispatch)
        .where(col(ExecutorDispatch.benchmark_id) == benchmark_id)
        .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
    )
    if except_dispatch_id is not None:
        dispatches = dispatches.where(col(ExecutorDispatch.id) != except_dispatch_id)
    session.exec(
        dispatches.values(
            status=ExecutorDispatchStatus.FAILED,
            finished_at=datetime.now(ZoneInfo("UTC")),
        )
    )


def active_dispatch_exists(
    session: Session,
    benchmark_id: UUID,
    *,
    except_dispatch_id: UUID | None = None,
) -> bool:
    """Return whether another queued or running dispatch exists."""
    dispatches = (
        select(ExecutorDispatch.id)
        .where(col(ExecutorDispatch.benchmark_id) == benchmark_id)
        .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
    )
    if except_dispatch_id is not None:
        dispatches = dispatches.where(col(ExecutorDispatch.id) != except_dispatch_id)
    return session.exec(dispatches).first() is not None


def _terminalize_dispatch_tasks(
    session: Session,
    *,
    benchmark: Benchmark,
    dispatch: ExecutorDispatch,
    task_ids: list[str],
    error_message: str,
    producer: str,
    operation: str,
    error_type: str,
    cause_code: str | None,
    finished_at: datetime,
) -> None:
    tasks = session.exec(
        select(Task)
        .where(col(Task.benchmark) == benchmark.id)
        .where(col(Task.org_id) == benchmark.org_id)
        .where(col(Task.task_id).in_(task_ids))
        .where(col(Task.started_at) <= dispatch.created_at)
        .where(
            col(Task.status).in_(
                (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
            )
        )
        .with_for_update()
    ).all()
    for task in tasks:
        session.add(
            ErrorResult(
                org_id=task.org_id,
                task=task.id,
                error_message=error_message,
                producer=producer,
                operation=operation,
                error_type=error_type,
                cause_code=cause_code,
            )
        )
        task.status = TaskStatus.ERROR
        task.finished_at = finished_at
        session.add(task)


def record_dispatch_failure(
    session: Session,
    *,
    benchmark: Benchmark,
    dispatch_id: UUID,
    task_ids: list[str],
    error_message: str,
    producer: str,
    operation: str,
    error_type: str,
    cause_code: str | None = None,
    failure_reason: str | None = None,
    only_if_lease_expired: bool = False,
) -> bool:
    """Record a dispatch failure without overwriting work admitted by a newer dispatch.

    Admission timestamps selected tasks before creating the dispatch, so its creation time
    is the durable upper bound for task executions owned by that dispatch.
    """
    benchmark = session.exec(
        select(Benchmark)
        .where(Benchmark.id == benchmark.id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one()

    dispatch_query = (
        select(ExecutorDispatch)
        .where(ExecutorDispatch.id == dispatch_id)
        .where(ExecutorDispatch.benchmark_id == benchmark.id)
        .where(ExecutorDispatch.status == ExecutorDispatchStatus.RUNNING)
    )
    if only_if_lease_expired:
        dispatch_query = dispatch_query.where(ExecutorDispatch.lease_expires_at <= func.current_timestamp())
    dispatch = session.exec(dispatch_query.with_for_update()).one_or_none()
    if dispatch is None:
        return False

    now = datetime.now(ZoneInfo("UTC"))
    sibling_active = active_dispatch_exists(session, benchmark.id, except_dispatch_id=dispatch_id)

    _terminalize_dispatch_tasks(
        session,
        benchmark=benchmark,
        dispatch=dispatch,
        task_ids=task_ids,
        error_message=error_message,
        producer=producer,
        operation=operation,
        error_type=error_type,
        cause_code=cause_code,
        finished_at=now,
    )
    dispatch.status = ExecutorDispatchStatus.FAILED
    dispatch.finished_at = now
    if failure_reason is not None:
        dispatch.failure_reason = failure_reason
    session.add(dispatch)
    if not sibling_active and benchmark.status == BenchmarkStatus.IN_PROGRESS:
        benchmark.status = BenchmarkStatus.ERROR
        benchmark.finished_at = now
        benchmark.error_message = error_message
        session.add(benchmark)
    return True


def reconcile_expired_dispatches(session: Session) -> int:
    """Fail running dispatches whose database-time lease was not renewed."""
    expired_dispatch_ids = session.exec(
        select(ExecutorDispatch.id)
        .where(ExecutorDispatch.status == ExecutorDispatchStatus.RUNNING)
        .where(ExecutorDispatch.lease_expires_at <= func.current_timestamp())
        .order_by(col(ExecutorDispatch.lease_expires_at), col(ExecutorDispatch.id))
    ).all()
    recovered_count = 0
    for dispatch_id in expired_dispatch_ids:
        dispatch = session.get(ExecutorDispatch, dispatch_id)
        if dispatch is None:
            continue
        benchmark = session.get(Benchmark, dispatch.benchmark_id)
        if benchmark is None:
            continue
        if record_dispatch_failure(
            session,
            benchmark=benchmark,
            dispatch_id=dispatch.id,
            task_ids=dispatch.assigned_task_ids or [],
            error_message="Executor dispatch lease expired",
            producer="executor_dispatch",
            operation="dispatch_reconciliation",
            error_type="ExecutorDispatchLeaseExpired",
            cause_code="LEASE_EXPIRED",
            failure_reason="LEASE_EXPIRED",
            only_if_lease_expired=True,
        ):
            recovered_count += 1
    return recovered_count


def resolve_enqueue_failure(
    session: Session,
    *,
    benchmark_id: UUID,
    dispatch_id: UUID,
    task_ids: list[str],
) -> EnqueueFailureResolution:
    """Fail an unclaimed dispatch without overriding delivered or superseding work."""
    benchmark = session.exec(
        select(Benchmark)
        .where(Benchmark.id == benchmark_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one()
    dispatch = session.exec(
        select(ExecutorDispatch)
        .where(ExecutorDispatch.id == dispatch_id)
        .where(ExecutorDispatch.benchmark_id == benchmark_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one()

    if dispatch.status != ExecutorDispatchStatus.QUEUED:
        resolution = (
            EnqueueFailureResolution.DELIVERED
            if dispatch.started_at is not None or dispatch.status == ExecutorDispatchStatus.FINISHED
            else EnqueueFailureResolution.SUPERSEDED
        )
        session.rollback()
        return resolution

    now = datetime.now(ZoneInfo("UTC"))
    error_message = "Executor dispatch enqueue failed"
    dispatch.status = ExecutorDispatchStatus.FAILED
    dispatch.finished_at = now
    session.add(dispatch)
    session.flush()

    if benchmark.status == BenchmarkStatus.IN_PROGRESS and not active_dispatch_exists(session, benchmark_id):
        benchmark.status = BenchmarkStatus.ERROR
        benchmark.finished_at = now
        benchmark.error_message = error_message
        session.add(benchmark)

    _terminalize_dispatch_tasks(
        session,
        benchmark=benchmark,
        dispatch=dispatch,
        task_ids=task_ids,
        error_message=error_message,
        producer="executor_dispatch",
        operation="enqueue",
        error_type="ExecutorDispatchEnqueueError",
        cause_code=None,
        finished_at=now,
    )
    session.commit()
    return EnqueueFailureResolution.FAILED
