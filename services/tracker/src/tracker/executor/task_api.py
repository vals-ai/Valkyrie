"""Task ownership, ordered mutations, and receipts shared by executor API versions."""

from collections.abc import Callable
from datetime import datetime
from uuid import UUID

from pydantic import JsonValue
from sqlmodel import Session, select

from tracker.database.models import (
    AgentCausedExitReason,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    ExecutorDispatch,
    ExecutorTaskAttempt,
    ExecutorTaskReceipt,
    ExecutorPoolReservation,
    Benchmark,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.executor.dispatch_api import DispatchConflict, as_utc, lock_claimed_dispatch
from tracker.observability.tracing import observability_span
from tracker.scheduler.store import claim_eligible_task

_RUNNABLE = (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)


def read_task_receipt(
    session: Session, dispatch_id: UUID, command_id: UUID, task_id: UUID, digest: str
) -> ExecutorTaskReceipt | None:
    receipt = session.get(ExecutorTaskReceipt, (dispatch_id, command_id))
    if receipt is not None and (receipt.task_id != task_id or receipt.request_digest != digest):
        raise DispatchConflict("Executor command ID was already used for a different request")

    return receipt


def _lock_task(session: Session, dispatch: ExecutorDispatch, task_id: UUID, started_at: datetime) -> Task:
    task = session.exec(
        select(Task)
        .where(Task.id == task_id, Task.benchmark == dispatch.benchmark_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    ).one_or_none()
    if task is None or dispatch.assigned_task_ids is None or task.task_id not in dispatch.assigned_task_ids:
        raise DispatchConflict("Task is not assigned to this executor dispatch")
    if as_utc(task.started_at) != as_utc(started_at) or as_utc(task.started_at) > as_utc(dispatch.created_at):
        raise DispatchConflict("Executor task attempt was superseded")

    return task


def _save_receipt(session: Session, owner: ExecutorTaskAttempt, command_id: UUID, digest: str) -> ExecutorTaskReceipt:
    receipt = ExecutorTaskReceipt(
        dispatch_id=owner.dispatch_id,
        command_id=command_id,
        task_id=owner.task_id,
        request_digest=digest,
        revision=owner.revision,
    )
    session.add(receipt)
    session.flush()

    return receipt


def claim_task(
    session: Session,
    dispatch_id: UUID,
    claimant_id: UUID,
    task_id: UUID,
    started_at: datetime,
    command_id: UUID,
    request_digest: str,
) -> ExecutorTaskReceipt:
    benchmark, dispatch, current = lock_claimed_dispatch(session, dispatch_id, claimant_id)
    previous = read_task_receipt(session, dispatch_id, command_id, task_id, request_digest)
    if previous is not None:
        return previous
    if not current or benchmark.status != BenchmarkStatus.IN_PROGRESS:
        raise DispatchConflict("Executor run cannot claim task attempts")
    task = _lock_task(session, dispatch, task_id, started_at)
    if task.org_id != benchmark.org_id:
        raise DispatchConflict("Task is not assigned to this executor organization")
    if task.status not in (TaskStatus.PENDING, TaskStatus.EVALUATING):
        raise DispatchConflict("Task attempt is not available for execution")
    if task.status == TaskStatus.EVALUATING and as_utc(task.started_at) != as_utc(dispatch.created_at):
        raise DispatchConflict("Evaluation resume belongs to a different dispatch")
    owner = session.get(ExecutorTaskAttempt, task.id)
    if owner is not None and as_utc(owner.started_at) == as_utc(task.started_at):
        if owner.dispatch_id != dispatch_id:
            raise DispatchConflict("Task attempt belongs to another executor dispatch")
    else:
        if owner is None:
            owner = ExecutorTaskAttempt(task_id=task.id, dispatch_id=dispatch_id, started_at=task.started_at)
        else:
            owner.dispatch_id = dispatch_id
            owner.started_at = task.started_at
            owner.revision = 0
        session.add(owner)

    return _save_receipt(session, owner, command_id, request_digest)


def write_task(
    session: Session,
    dispatch_id: UUID,
    claimant_id: UUID,
    task_id: UUID,
    started_at: datetime,
    command_id: UUID,
    request_digest: str,
    expected_revision: int,
    apply: Callable[[Session, Task], None],
    *,
    allow_stopping: bool = False,
) -> ExecutorTaskReceipt:
    benchmark, dispatch, current = lock_claimed_dispatch(session, dispatch_id, claimant_id)
    previous = read_task_receipt(session, dispatch_id, command_id, task_id, request_digest)
    if previous is not None:
        return previous
    permitted_statuses = (
        (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING) if allow_stopping else (BenchmarkStatus.IN_PROGRESS,)
    )
    if not current or benchmark.status not in permitted_statuses:
        raise DispatchConflict("Executor task write authority was revoked")
    task = _lock_task(session, dispatch, task_id, started_at)
    if task.org_id != benchmark.org_id:
        raise DispatchConflict("Task is not assigned to this executor organization")
    owner = session.get(ExecutorTaskAttempt, task.id)
    if owner is None or owner.dispatch_id != dispatch_id or as_utc(owner.started_at) != as_utc(task.started_at):
        raise DispatchConflict("Executor does not own this task attempt")
    if owner.revision != expected_revision:
        raise DispatchConflict("Executor task write revision is stale")
    apply(session, task)
    owner.revision += 1
    session.add_all([task, owner])

    return _save_receipt(session, owner, command_id, request_digest)


def set_task_status(session: Session, task: Task, *, status: TaskStatus, expected: tuple[TaskStatus, ...]) -> None:
    with observability_span(
        "task.status_transition",
        task_id=task.task_id,
        benchmark_id=str(task.benchmark),
        from_status=task.status.value,
        to_status=status.value,
    ):
        if task.status not in expected:
            raise DispatchConflict("Task status does not permit this operation")
        if status in (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS):
            _require_queue_reservation(session, task, status)
        task.status = status
        session.add(task)


def _require_queue_reservation(session: Session, task: Task, status: TaskStatus) -> None:
    benchmark = session.get(Benchmark, task.benchmark)
    assert benchmark is not None
    pool_id = benchmark.arguments.queue_pool_id
    if pool_id is None:
        return
    reservation = session.exec(
        select(ExecutorPoolReservation).where(ExecutorPoolReservation.pool_id == pool_id).with_for_update()
    ).one_or_none()
    owner = session.get(ExecutorTaskAttempt, task.id)
    if (
        reservation is None
        or owner is None
        or reservation.dispatch_id != owner.dispatch_id
        or reservation.task_id != task.id
        or as_utc(reservation.started_at) != as_utc(task.started_at)
    ):
        raise DispatchConflict("Queued sandbox creation requires this task's pool reservation")
    if status == TaskStatus.BUILDING and not claim_eligible_task(session, pool_id, task.id, task.started_at):
        raise DispatchConflict("Queued task is no longer eligible for sandbox creation")


def begin_evaluation(
    session: Session, task: Task, *, sandbox_build_duration: float | None, agent_run_duration: float | None
) -> None:
    set_task_status(session, task, status=TaskStatus.EVALUATING, expected=(TaskStatus.IN_PROGRESS,))
    breakdown = TaskBreakdown(sandbox_build_duration=sandbox_build_duration, agent_run_duration=agent_run_duration)
    session.add(breakdown)
    session.flush()
    task.task_breakdown = breakdown.id


def save_checkpoint(session: Session, task: Task, *, checkpoint: dict[str, JsonValue]) -> None:
    if task.status != TaskStatus.EVALUATING:
        raise DispatchConflict("Only an evaluating task can save a checkpoint")
    task.eval_resume_state = checkpoint
    session.add(task)


def complete_task(
    session: Session,
    task: Task,
    *,
    result: dict[str, JsonValue],
    instance_id: str | None,
    exit_reason: AgentCausedExitReason | None,
    evaluation_run_duration: float | None,
    sandbox_run_duration: float | None,
) -> None:
    set_task_status(session, task, status=TaskStatus.FINISHED, expected=(TaskStatus.EVALUATING, TaskStatus.ERROR))
    session.add(
        EvaluationResult(
            org_id=task.org_id,
            task=task.id,
            result=result,
            instance_id=instance_id,
            agent_caused_exit_reason=exit_reason,
        )
    )
    if task.task_breakdown is not None:
        breakdown = session.get(TaskBreakdown, task.task_breakdown)
        if breakdown is not None:
            if evaluation_run_duration is not None:
                breakdown.evaluation_run_duration = evaluation_run_duration
            if sandbox_run_duration is not None:
                breakdown.sandbox_run_duration = sandbox_run_duration
            session.add(breakdown)


def record_task_error(
    session: Session,
    task: Task,
    *,
    error_message: str,
    producer: str,
    operation: str,
    error_type: str,
    cause_code: str | None,
    retry_scheduled: bool,
    failed_attempt_number: int | None,
) -> None:
    if task.status not in _RUNNABLE:
        raise DispatchConflict("Task is no longer runnable")
    session.add(
        ErrorResult(
            org_id=task.org_id,
            task=task.id,
            error_message=error_message,
            producer=producer,
            operation=operation,
            error_type=error_type,
            cause_code=cause_code,
            retry_scheduled=retry_scheduled,
            failed_attempt_number=failed_attempt_number,
        )
    )
    if not retry_scheduled:
        task.status = TaskStatus.ERROR
        session.add(task)
