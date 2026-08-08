"""Focused transaction and authority tests for task execution persistence.

Run: uv run pytest tests/unit/database/repositories/test_task_execution_repository.py
"""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlmodel import Session, select

from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    ErrorResult,
    EvaluationResult,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Org,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.database.repositories import TaskExecutionRepository
from tracker.executor.execution_authority import ExecutionAuthority


def _task_with_authority(
    session: Session,
    executor_authority: Callable[..., ExecutionAuthority],
    *,
    status: TaskStatus = TaskStatus.IN_PROGRESS,
) -> tuple[Task, ExecutionAuthority]:
    benchmark = make_benchmark(session=session)
    task = make_task(benchmark, "task", status=status)
    session.add(task)
    session.commit()
    authority = executor_authority(benchmark)
    return task, authority


def test_lock_execution_authority_returns_locked_benchmark(
    database_session: Session, executor_authority: Callable[..., ExecutionAuthority]
) -> None:
    task, authority = _task_with_authority(database_session, executor_authority)
    repository = TaskExecutionRepository(database_session)

    assert repository.lock_execution_authority(authority).id == task.benchmark
    database_session.rollback()


def test_revoked_authority_rolls_back_error_row(
    database_session: Session, executor_authority: Callable[..., ExecutionAuthority]
) -> None:
    """Authority revocation rejects and removes the staged terminal error."""
    task, authority = _task_with_authority(database_session, executor_authority)
    dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
    assert dispatch is not None
    dispatch.status = ExecutorDispatchStatus.FAILED
    database_session.commit()

    assert TaskExecutionRepository(database_session).record_error(task.id, task.org_id, "revoked", authority) is False
    assert database_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).all() == []
    database_session.expire_all()
    assert database_session.get(Task, task.id).status == TaskStatus.IN_PROGRESS  # type: ignore[union-attr]


def test_stale_attempt_rolls_back_error_row(
    database_session: Session, executor_authority: Callable[..., ExecutionAuthority]
) -> None:
    """A stale attempt cannot leave an error row behind."""
    started_at = datetime.now(UTC)
    task, authority = _task_with_authority(database_session, executor_authority)
    assert (
        TaskExecutionRepository(database_session).record_error(
            task.id,
            task.org_id,
            "stale",
            authority,
            expected_started_at=started_at - timedelta(seconds=1),
        )
        is False
    )
    assert database_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).all() == []


def test_stopped_task_rejects_non_stop_writes(
    database_session: Session, executor_authority: Callable[..., ExecutionAuthority]
) -> None:
    """Stopped tasks reject transitions and resume-state writes."""
    task, authority = _task_with_authority(database_session, executor_authority, status=TaskStatus.STOPPED)
    repository = TaskExecutionRepository(database_session)

    assert repository.transition_status(task.id, task.org_id, TaskStatus.BUILDING, authority) is False
    assert repository.save_eval_resume_state(task.id, task.org_id, {"cursor": 1}, authority) is False
    database_session.expire_all()
    persisted = database_session.get(Task, task.id)
    assert persisted is not None
    assert persisted.status == TaskStatus.STOPPED
    assert persisted.eval_resume_state is None


def test_error_write_is_atomic_and_caller_owns_commit(
    database_session: Session, executor_authority: Callable[..., ExecutionAuthority]
) -> None:
    """Successful error persistence stays uncommitted until the caller commits."""
    task, authority = _task_with_authority(database_session, executor_authority)
    repository = TaskExecutionRepository(database_session)

    assert repository.record_error(task.id, task.org_id, "failure", authority) is True
    assert database_session.in_transaction()
    database_session.rollback()
    assert database_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).all() == []

    assert repository.record_error(task.id, task.org_id, "failure", authority) is True
    database_session.commit()
    database_session.expire_all()
    assert database_session.get(Task, task.id).status == TaskStatus.ERROR  # type: ignore[union-attr]
    assert len(database_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).all()) == 1


def test_dangling_evaluation_breakdown_rolls_back_all_writes(
    database_session: Session, executor_authority: Callable[..., ExecutionAuthority]
) -> None:
    """A dangling evaluation breakdown rejects the result without partial writes."""
    task, authority = _task_with_authority(database_session, executor_authority, status=TaskStatus.EVALUATING)
    dangling_breakdown_id = uuid4()
    task.task_breakdown = dangling_breakdown_id
    database_session.commit()
    result = EvaluationResult(org_id=task.org_id, task=task.id, result={"score": 1})
    repository = TaskExecutionRepository(database_session)

    assert (
        repository.record_evaluation_and_finish(
            task.id,
            task.org_id,
            result,
            authority,
            expected_started_at=task.started_at,
            evaluation_run_duration=2.0,
            sandbox_run_duration=3.0,
        )
        is False
    )
    assert not database_session.in_transaction()

    database_session.expire_all()
    persisted = database_session.get(Task, task.id)
    assert persisted is not None
    assert persisted.status == TaskStatus.EVALUATING
    assert persisted.finished_at is None
    assert persisted.task_breakdown == dangling_breakdown_id
    assert database_session.exec(select(EvaluationResult).where(EvaluationResult.task == task.id)).all() == []
    assert database_session.get(TaskBreakdown, dangling_breakdown_id) is None


def test_evaluation_breakdown_and_status_are_atomic(
    database_session: Session, executor_authority: Callable[..., ExecutionAuthority]
) -> None:
    """Evaluation, duration updates, and finishing status share one transaction."""
    task, authority = _task_with_authority(database_session, executor_authority, status=TaskStatus.EVALUATING)
    breakdown = TaskBreakdown(agent_run_duration=1.0)
    task.task_breakdown = breakdown.id
    database_session.add(breakdown)
    database_session.commit()
    result = EvaluationResult(org_id=task.org_id, task=task.id, result={"score": 1})
    repository = TaskExecutionRepository(database_session)

    assert (
        repository.record_evaluation_and_finish(
            task.id,
            task.org_id,
            result,
            authority,
            expected_started_at=task.started_at,
            evaluation_run_duration=2.0,
            sandbox_run_duration=3.0,
        )
        is True
    )
    database_session.rollback()
    database_session.expire_all()
    persisted = database_session.get(Task, task.id)
    assert persisted is not None
    assert persisted.status == TaskStatus.EVALUATING
    assert database_session.exec(select(EvaluationResult).where(EvaluationResult.task == task.id)).all() == []
    persisted_breakdown = database_session.get(TaskBreakdown, breakdown.id)
    assert persisted_breakdown is not None
    assert persisted_breakdown.evaluation_run_duration is None

    result = EvaluationResult(org_id=task.org_id, task=task.id, result={"score": 1})
    assert (
        repository.record_evaluation_and_finish(
            task.id,
            task.org_id,
            result,
            authority,
            expected_started_at=task.started_at,
            evaluation_run_duration=2.0,
            sandbox_run_duration=3.0,
        )
        is True
    )
    database_session.commit()
    database_session.expire_all()
    assert database_session.get(Task, task.id).status == TaskStatus.FINISHED  # type: ignore[union-attr]
    persisted_breakdown = database_session.get(TaskBreakdown, breakdown.id)
    assert persisted_breakdown is not None
    assert persisted_breakdown.evaluation_run_duration == 2.0
    assert persisted_breakdown.sandbox_run_duration == 3.0


def test_task_lookup_and_writes_are_organization_scoped(
    database_session: Session, executor_authority: Callable[..., ExecutionAuthority]
) -> None:
    """Foreign organizations cannot read or mutate an execution task."""
    task, authority = _task_with_authority(database_session, executor_authority)
    other_org = Org(id=uuid4(), name="other")
    database_session.add(other_org)
    database_session.commit()
    repository = TaskExecutionRepository(database_session)

    assert repository.get_for_execution(task.id, other_org.id) is None
    assert repository.get_for_benchmark(task.benchmark, task.task_id, other_org.id) is None
    assert repository.transition_status(task.id, other_org.id, TaskStatus.BUILDING, authority) is False
    database_session.expire_all()
    assert database_session.get(Task, task.id).status == TaskStatus.IN_PROGRESS  # type: ignore[union-attr]


def test_is_current_checks_attempt_and_authority(
    database_session: Session, executor_authority: Callable[..., ExecutionAuthority]
) -> None:
    """Current-attempt checks reject stale started-at values and revoked dispatches."""
    task, authority = _task_with_authority(database_session, executor_authority)
    repository = TaskExecutionRepository(database_session)

    assert repository.is_current(task.id, task.org_id, authority, task.started_at) is True
    assert repository.is_current(task.id, task.org_id, authority, datetime.now(UTC)) is False

    dispatch = database_session.get(ExecutorDispatch, authority.dispatch_id)
    assert dispatch is not None
    dispatch.status = ExecutorDispatchStatus.FAILED
    database_session.commit()
    assert repository.is_current(task.id, task.org_id, authority, task.started_at) is False
