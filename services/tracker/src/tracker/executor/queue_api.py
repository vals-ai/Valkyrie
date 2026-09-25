"""Persistent creation reservations and compatibility fences for executor queues."""

from datetime import datetime
from functools import partial
from uuid import UUID

from sqlalchemy import JSON, and_, type_coerce
from sqlmodel import Session, col, select

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchAccess,
    ExecutorDispatchStatus,
    ExecutorPoolReservation,
    ExecutorTaskReceipt,
    Task,
    TaskStatus,
)
from tracker.executor.dispatch_api import DispatchConflict, as_utc, lock_claimed_dispatch
from tracker.executor.release_control import get_executor_admission
from tracker.executor.task_api import read_task_receipt, write_task
from tracker.scheduler.store import eligible_task_is, try_queue_pool_transaction_lock


def _legacy_work_exists(session: Session, pool_id: str) -> bool:
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    legacy = (
        select(col(Benchmark.id))
        .outerjoin(
            ExecutorDispatch,
            and_(
                col(ExecutorDispatch.benchmark_id) == col(Benchmark.id),
                col(ExecutorDispatch.status).in_((ExecutorDispatchStatus.QUEUED, ExecutorDispatchStatus.RUNNING)),
            ),
        )
        .outerjoin(ExecutorDispatchAccess, col(ExecutorDispatchAccess.dispatch_id) == col(ExecutorDispatch.id))
        .where(arguments["queue_pool_id"].as_string() == pool_id)
        .where(col(Benchmark.status).in_((BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)))
        .where(col(ExecutorDispatchAccess.dispatch_id).is_(None))
        .limit(1)
    )

    return session.exec(legacy).first() is not None


def _reserve(session: Session, task: Task, *, pool_id: str, dispatch_id: UUID, reservation_id: UUID) -> None:
    if task.status != TaskStatus.PENDING:
        raise DispatchConflict("Only pending tasks may reserve sandbox creation")
    session.add(
        ExecutorPoolReservation(
            pool_id=pool_id,
            reservation_id=reservation_id,
            dispatch_id=dispatch_id,
            task_id=task.id,
            started_at=task.started_at,
        )
    )


def reserve_pool(
    session: Session,
    dispatch_id: UUID,
    claimant_id: UUID,
    task_id: UUID,
    started_at: datetime,
    command_id: UUID,
    request_digest: str,
    expected_revision: int,
) -> ExecutorTaskReceipt | None:
    # Serialize with start/retry admission, without closing work already running during a deploy fence.
    get_executor_admission(session, for_update=True)
    benchmark, dispatch, current = lock_claimed_dispatch(session, dispatch_id, claimant_id)
    if not current or benchmark.status != BenchmarkStatus.IN_PROGRESS:
        raise DispatchConflict("Executor cannot reserve sandbox creation after authority was revoked")
    pool_id = benchmark.arguments.queue_pool_id
    if pool_id is None:
        raise DispatchConflict("Run does not use queued sandbox creation")
    reservation = session.get(ExecutorPoolReservation, pool_id)
    previous = read_task_receipt(session, dispatch_id, command_id, task_id, request_digest)
    if previous is not None:
        if reservation is None or reservation.reservation_id != command_id or reservation.dispatch_id != dispatch_id:
            raise DispatchConflict("Sandbox creation reservation was already released")
        return previous
    if reservation is not None or _legacy_work_exists(session, pool_id):
        return None
    if not try_queue_pool_transaction_lock(session, pool_id):
        return None
    if dispatch.assigned_task_ids is None:
        raise DispatchConflict("Executor dispatch has no persisted task assignment")
    if not eligible_task_is(session, pool_id, task_id, as_utc(started_at).replace(tzinfo=None)):
        return None

    return write_task(
        session,
        dispatch_id,
        claimant_id,
        task_id,
        started_at,
        command_id,
        request_digest,
        expected_revision,
        partial(_reserve, pool_id=pool_id, dispatch_id=dispatch_id, reservation_id=command_id),
    )


def release_pool(
    session: Session,
    dispatch_id: UUID,
    claimant_id: UUID,
    task_id: UUID,
    started_at: datetime,
    command_id: UUID,
    request_digest: str,
    reservation_id: UUID,
) -> None:
    """Release only after creation and any required cleanup have settled, even if the lease expired."""
    get_executor_admission(session, for_update=True)
    lock_claimed_dispatch(session, dispatch_id, claimant_id)
    previous = read_task_receipt(session, dispatch_id, command_id, task_id, request_digest)
    if previous is not None:
        return
    reservation = session.exec(
        select(ExecutorPoolReservation)
        .where(
            ExecutorPoolReservation.dispatch_id == dispatch_id,
            ExecutorPoolReservation.task_id == task_id,
            ExecutorPoolReservation.reservation_id == reservation_id,
        )
        .with_for_update()
    ).one_or_none()
    if reservation is None or as_utc(reservation.started_at) != as_utc(started_at):
        raise DispatchConflict("Executor does not hold this sandbox creation reservation")
    reserved = session.get(ExecutorTaskReceipt, (dispatch_id, reservation_id))
    assert reserved is not None
    session.delete(reservation)
    session.add(
        ExecutorTaskReceipt(
            dispatch_id=dispatch_id,
            command_id=command_id,
            task_id=task_id,
            request_digest=request_digest,
            revision=reserved.revision,
        )
    )
    session.flush()
