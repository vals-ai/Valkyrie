"""Canonical transaction owners for executor dispatch lifecycle."""

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import Any, NamedTuple
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import and_, delete, func, or_, update
from sqlmodel import Session, col, select

from executor_protocol import (
    DATASET_VERSION_PROTOCOL_VERSION,
    MANAGED_EXECUTION_PROTOCOL_VERSION,
    RESERVED_QUEUE_PROTOCOL_VERSION,
)
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchPayload,
    ExecutorDispatchStatus,
    ExecutorRelease,
    FailureCategory,
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
from tracker.scheduler.store import is_reserved_queue_pool_id, reserved_queue_pool_id

_ACTIVE_DISPATCH_STATUSES = (
    ExecutorDispatchStatus.QUEUED,
    ExecutorDispatchStatus.RUNNING,
)


class EnqueueFailureResolution(str, Enum):
    FAILED = "FAILED"
    DELIVERED = "DELIVERED"
    SUPERSEDED = "SUPERSEDED"


def _require_managed_execution_release(release: ExecutorRelease) -> None:
    if release.protocol_version not in {
        MANAGED_EXECUTION_PROTOCOL_VERSION,
        DATASET_VERSION_PROTOCOL_VERSION,
        RESERVED_QUEUE_PROTOCOL_VERSION,
    }:
        raise ReleaseControlError("Activate an executor release that supports managed runs")


def _require_reserved_queue_release(release: ExecutorRelease) -> None:
    if release.protocol_version != RESERVED_QUEUE_PROTOCOL_VERSION:
        raise ReleaseControlError("Activate an executor release that supports reserved queues")


def _require_compatible_release(benchmark: Benchmark, release: ExecutorRelease) -> None:
    if benchmark.arguments.dataset_version is not None and release.protocol_version not in {
        DATASET_VERSION_PROTOCOL_VERSION,
        RESERVED_QUEUE_PROTOCOL_VERSION,
    }:
        raise ReleaseControlError("Activate an executor release that supports pinned dataset versions")
    if benchmark.arguments.environment == "aws":
        _require_managed_execution_release(release)
    queue_pool_id = benchmark.arguments.queue_pool_id
    if queue_pool_id is not None and is_reserved_queue_pool_id(queue_pool_id):
        _require_reserved_queue_release(release)


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
    queue_pool_id = benchmark.arguments.queue_pool_id
    if queue_pool_id is not None:
        _require_reserved_queue_release(release)
        benchmark.arguments = benchmark.arguments.model_copy(
            update={"queue_pool_id": reserved_queue_pool_id(queue_pool_id)}
        )
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


def active_dispatch_exists(session: Session, benchmark_id: UUID) -> bool:
    """Return whether a queued or running dispatch exists for the benchmark."""
    dispatches = (
        select(ExecutorDispatch.id)
        .where(col(ExecutorDispatch.benchmark_id) == benchmark_id)
        .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
    )
    return session.exec(dispatches).first() is not None


@dataclass(frozen=True)
class _DispatchError:
    error_message: str
    producer: str
    operation: str
    error_type: str
    category: FailureCategory
    cause_code: str | None = None


class _Expiry(NamedTuple):
    deadline: Any
    reason: str
    message: str
    error_type: str


_EXPIRIES = {
    ExecutorDispatchStatus.RUNNING: _Expiry(
        col(ExecutorDispatch.lease_expires_at),
        "LEASE_EXPIRED",
        "Executor dispatch lease expired",
        "ExecutorDispatchLeaseExpired",
    ),
    ExecutorDispatchStatus.QUEUED: _Expiry(
        col(ExecutorDispatch.claim_deadline_at),
        "CLAIM_DEADLINE_EXPIRED",
        "Executor dispatch claim deadline expired",
        "ExecutorDispatchClaimDeadlineExpired",
    ),
}


def stop_unclaimed_dispatches(session: Session, benchmark: Benchmark) -> tuple[bool, list[str]]:
    """Revoke queued runners under the benchmark lock held by the stop transition."""
    dispatches = session.exec(
        select(ExecutorDispatch)
        .where(ExecutorDispatch.benchmark_id == benchmark.id)
        .where(ExecutorDispatch.status == ExecutorDispatchStatus.QUEUED)
        .with_for_update()
    ).all()
    arns = []
    for dispatch in dispatches:
        dispatch.status = ExecutorDispatchStatus.FAILED
        dispatch.finished_at = datetime.now(ZoneInfo("UTC"))
        dispatch.failure_reason = "STOPPED_BEFORE_CLAIM"
        session.add(dispatch)
        session.exec(delete(ExecutorDispatchPayload).where(ExecutorDispatchPayload.dispatch_id == dispatch.id))
        if dispatch.ecs_task_arn is not None:
            arns.append(dispatch.ecs_task_arn)
    return bool(dispatches), arns


def _terminalize_dispatch_tasks(
    session: Session,
    *,
    benchmark: Benchmark,
    dispatch: ExecutorDispatch,
    task_ids: list[str],
    error: _DispatchError,
    finished_at: datetime,
) -> bool:
    """Error the tasks this dispatch owned and return whether a sibling dispatch is still active."""
    assignments = session.exec(
        select(col(ExecutorDispatch.assigned_task_ids))
        .where(ExecutorDispatch.benchmark_id == benchmark.id)
        .where(ExecutorDispatch.id != dispatch.id)
        .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
    ).all()
    failed_task_attempts = and_(
        col(Task.status) == TaskStatus.EVALUATING,
        col(Task.started_at) == dispatch.created_at,
    )
    if None not in assignments:
        sibling_task_ids = {task_id for assignment in assignments for task_id in assignment or ()}
        failed_task_attempts = or_(
            failed_task_attempts,
            and_(
                col(Task.status).in_(
                    (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
                ),
                col(Task.started_at) <= dispatch.created_at,
                col(Task.task_id).not_in(sibling_task_ids),
            ),
        )

    tasks = session.exec(
        select(Task)
        .where(col(Task.benchmark) == benchmark.id)
        .where(col(Task.org_id) == benchmark.org_id)
        .where(col(Task.task_id).in_(task_ids))
        .where(failed_task_attempts)
        .with_for_update()
    ).all()
    for task in tasks:
        session.add(ErrorResult(org_id=task.org_id, task=task.id, **asdict(error)))
        task.status = TaskStatus.ERROR
        task.finished_at = finished_at
        session.add(task)
    return bool(assignments)


def record_dispatch_failure(
    session: Session,
    *,
    benchmark_id: UUID,
    dispatch_id: UUID,
    task_ids: list[str],
    error_message: str,
    producer: str,
    operation: str,
    error_type: str,
    category: FailureCategory,
    cause_code: str | None = None,
    failure_reason: str | None = None,
    dispatch_status: ExecutorDispatchStatus = ExecutorDispatchStatus.RUNNING,
    only_if_expired: bool = False,
) -> bool:
    """Record a dispatch failure without overwriting newer admitted work.

    Resumable evaluations carry the dispatch creation time as their exact ownership token. Other attempts
    remain protected while an active sibling can run them, or its persisted assignment is unavailable.
    """
    benchmark = session.exec(
        select(Benchmark)
        .where(Benchmark.id == benchmark_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    ).one()

    dispatch_query = (
        select(ExecutorDispatch)
        .where(ExecutorDispatch.id == dispatch_id)
        .where(ExecutorDispatch.benchmark_id == benchmark_id)
        .where(ExecutorDispatch.status == dispatch_status)
    )
    if only_if_expired:
        dispatch_query = dispatch_query.where(_EXPIRIES[dispatch_status].deadline <= func.current_timestamp())
    dispatch = session.exec(dispatch_query.with_for_update()).one_or_none()
    if dispatch is None:
        return False

    now = datetime.now(ZoneInfo("UTC"))
    sibling_active = _terminalize_dispatch_tasks(
        session,
        benchmark=benchmark,
        dispatch=dispatch,
        task_ids=task_ids,
        error=_DispatchError(error_message, producer, operation, error_type, category, cause_code),
        finished_at=now,
    )
    dispatch.status = ExecutorDispatchStatus.FAILED
    dispatch.finished_at = now
    if failure_reason is not None:
        dispatch.failure_reason = failure_reason
    session.add(dispatch)
    if not sibling_active:
        if benchmark.status == BenchmarkStatus.IN_PROGRESS:
            benchmark.status = BenchmarkStatus.ERROR
            benchmark.finished_at = now
            benchmark.error_message = error_message
            session.add(benchmark)
        elif benchmark.status == BenchmarkStatus.STOPPING:
            benchmark.status = BenchmarkStatus.STOPPED
            benchmark.finished_at = now
            session.add(benchmark)
    return True


def reconcile_expired_dispatches(session: Session) -> int:
    """Fail running or queued dispatches whose database-time owner window expired."""
    recovered_count = 0
    for status, expiry in _EXPIRIES.items():
        expired_dispatches = session.exec(
            select(ExecutorDispatch.id, ExecutorDispatch.benchmark_id, col(ExecutorDispatch.assigned_task_ids))
            .where(ExecutorDispatch.status == status)
            .where(expiry.deadline <= func.current_timestamp())
            .order_by(expiry.deadline, col(ExecutorDispatch.id))
        ).all()
        for dispatch_id, benchmark_id, assigned_task_ids in expired_dispatches:
            recovered_count += record_dispatch_failure(
                session,
                benchmark_id=benchmark_id,
                dispatch_id=dispatch_id,
                task_ids=assigned_task_ids or [],
                error_message=expiry.message,
                producer="executor_dispatch",
                operation="dispatch_reconciliation",
                error_type=expiry.error_type,
                category=FailureCategory.INFRASTRUCTURE,
                cause_code=expiry.reason,
                failure_reason=expiry.reason,
                dispatch_status=status,
                only_if_expired=True,
            )
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
        session.rollback()
        if dispatch.started_at is not None or dispatch.status == ExecutorDispatchStatus.FINISHED:
            return EnqueueFailureResolution.DELIVERED
        return EnqueueFailureResolution.SUPERSEDED

    now = datetime.now(ZoneInfo("UTC"))
    error = _DispatchError(
        "Executor dispatch enqueue failed",
        "executor_dispatch",
        "enqueue",
        "ExecutorDispatchEnqueueError",
        FailureCategory.INFRASTRUCTURE,
    )
    dispatch.status = ExecutorDispatchStatus.FAILED
    dispatch.finished_at = now
    session.add(dispatch)
    sibling_active = _terminalize_dispatch_tasks(
        session, benchmark=benchmark, dispatch=dispatch, task_ids=task_ids, error=error, finished_at=now
    )
    if benchmark.status == BenchmarkStatus.IN_PROGRESS and not sibling_active:
        benchmark.status = BenchmarkStatus.ERROR
        benchmark.finished_at = now
        benchmark.error_message = error.error_message
        session.add(benchmark)
    session.commit()
    return EnqueueFailureResolution.FAILED
