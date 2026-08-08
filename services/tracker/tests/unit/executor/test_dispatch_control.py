"""Canonical durable dispatch-intent transaction contracts."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from sqlmodel import Session

from tracker.database.repositories import EnqueueFailureResolution, ExecutorControlRepository, TaskExecutionRepository
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Task,
    TaskStatus,
)
from tracker.executor.dispatch_control import admit_recovery_dispatch, admit_start_dispatch as _admit_start_dispatch
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.executor.release_control import (
    create_executor_dispatch,
    pin_benchmark_to_release,
    validate_release_manifest,
)


def register_release(session: Session, release: ExecutorRelease) -> ExecutorRelease:
    validate_release_manifest(release)
    return ExecutorControlRepository(session).register_release(release)


def promote_release(session: Session, release_id: str) -> ExecutorRelease:
    return ExecutorControlRepository(session).promote_release(release_id)


def admit_start_dispatch(session: Session, **kwargs: Any) -> ExecutorDispatch:
    return _admit_start_dispatch(
        session,
        executor_control_repository=ExecutorControlRepository(session),
        **kwargs,
    )


def _release(release_id: str) -> ExecutorRelease:
    return ExecutorRelease(
        id=release_id,
        artifact_uri=f"s3://artifacts/{release_id}.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
        created_at=datetime.now(UTC),
    )


def test_start_admission_selects_active_release(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    register_release(database_session, _release("active"))
    promote_release(database_session, "active")
    database_session.commit()
    dispatch_id = uuid4()

    dispatch = admit_start_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        dispatch_id=dispatch_id,
    )
    database_session.commit()

    assert example_benchmark_object.current_execution_release_id == "active"
    assert dispatch.id == dispatch_id
    assert dispatch.executor_release_id == "active"


def test_in_progress_retry_keeps_release_and_original_dispatch_active(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("current")
    register_release(database_session, release)
    promote_release(database_session, release.id)
    pin_benchmark_to_release(example_benchmark_object, release)
    database_session.add(example_benchmark_object)
    old_dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    old_dispatch.status = ExecutorDispatchStatus.RUNNING
    old_dispatch.started_at = datetime.now(UTC)
    database_session.add(old_dispatch)
    database_session.commit()

    dispatch = admit_recovery_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        pre_action_status=BenchmarkStatus.IN_PROGRESS,
        dispatch_id=uuid4(),
        kind=ExecutorDispatchKind.RETRY,
        executor_control_repository=ExecutorControlRepository(database_session),
    )
    database_session.commit()
    database_session.refresh(old_dispatch)

    assert example_benchmark_object.current_execution_release_id == release.id
    assert dispatch.executor_release_id == release.id
    assert old_dispatch.status == ExecutorDispatchStatus.RUNNING
    assert old_dispatch.finished_at is None


def test_enqueue_failure_makes_start_retryable(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    register_release(database_session, _release("active"))
    promote_release(database_session, "active")
    task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="task-1",
        status=TaskStatus.PENDING,
    )
    dispatch = admit_start_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        dispatch_id=uuid4(),
    )
    database_session.add(task)
    database_session.commit()

    resolution = ExecutorControlRepository(database_session).resolve_enqueue_failure(
        benchmark_id=example_benchmark_object.id,
        dispatch_id=dispatch.id,
        task_ids=[task.task_id],
    )
    database_session.refresh(example_benchmark_object)
    database_session.refresh(dispatch)
    database_session.refresh(task)

    assert resolution == EnqueueFailureResolution.FAILED
    assert example_benchmark_object.status == BenchmarkStatus.ERROR
    assert dispatch.status == ExecutorDispatchStatus.FAILED
    assert task.status == TaskStatus.ERROR


def test_additive_retry_enqueue_failure_keeps_original_execution_active(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("active")
    register_release(database_session, release)
    promote_release(database_session, release.id)
    pin_benchmark_to_release(example_benchmark_object, release)
    retry_task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="retry-task",
        status=TaskStatus.PENDING,
    )
    original_task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="original-task",
        status=TaskStatus.IN_PROGRESS,
    )
    stopped_task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="stopped-task",
        status=TaskStatus.STOPPED,
    )
    original_dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    original_dispatch.status = ExecutorDispatchStatus.RUNNING
    original_dispatch.started_at = datetime.now(UTC)
    dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.RETRY,
        dispatch_id=uuid4(),
    )
    newer_retry_task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="newer-retry-task",
        status=TaskStatus.PENDING,
        started_at=dispatch.created_at + timedelta(seconds=1),
    )
    database_session.add(example_benchmark_object)
    database_session.add(retry_task)
    database_session.add(original_task)
    database_session.add(stopped_task)
    database_session.add(newer_retry_task)
    database_session.add(original_dispatch)
    database_session.add(dispatch)
    database_session.commit()

    resolution = ExecutorControlRepository(database_session).resolve_enqueue_failure(
        benchmark_id=example_benchmark_object.id,
        dispatch_id=dispatch.id,
        task_ids=[retry_task.task_id, stopped_task.task_id, newer_retry_task.task_id],
    )
    database_session.refresh(example_benchmark_object)
    database_session.refresh(retry_task)
    database_session.refresh(original_task)
    database_session.refresh(stopped_task)
    database_session.refresh(newer_retry_task)

    assert resolution == EnqueueFailureResolution.FAILED
    assert example_benchmark_object.status == BenchmarkStatus.IN_PROGRESS
    assert retry_task.status == TaskStatus.ERROR
    assert original_task.status == TaskStatus.IN_PROGRESS
    assert stopped_task.status == TaskStatus.STOPPED
    assert newer_retry_task.status == TaskStatus.PENDING


def test_enqueue_failure_does_not_override_claimed_delivery(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    register_release(database_session, _release("active"))
    promote_release(database_session, "active")
    dispatch = admit_start_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        dispatch_id=uuid4(),
    )
    dispatch.status = ExecutorDispatchStatus.RUNNING
    dispatch.started_at = datetime.now(UTC)
    database_session.add(dispatch)
    database_session.commit()

    resolution = ExecutorControlRepository(database_session).resolve_enqueue_failure(
        benchmark_id=example_benchmark_object.id,
        dispatch_id=dispatch.id,
        task_ids=[],
    )
    database_session.refresh(example_benchmark_object)

    assert resolution == EnqueueFailureResolution.DELIVERED
    assert example_benchmark_object.status == BenchmarkStatus.IN_PROGRESS


def test_running_dispatch_failure_preserves_active_sibling(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("active")
    register_release(database_session, release)
    pin_benchmark_to_release(example_benchmark_object, release)
    failing_dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.RETRY,
        dispatch_id=uuid4(),
    )
    sibling_dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    for dispatch in (failing_dispatch, sibling_dispatch):
        dispatch.status = ExecutorDispatchStatus.RUNNING
        dispatch.started_at = datetime.now(UTC)
        database_session.add(dispatch)
    retry_task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="retry-task",
        status=TaskStatus.IN_PROGRESS,
        started_at=failing_dispatch.created_at - timedelta(seconds=1),
    )
    newer_retry_task = Task(
        org_id=example_benchmark_object.org_id,
        benchmark=example_benchmark_object.id,
        task_id="newer-retry-task",
        status=TaskStatus.PENDING,
        started_at=failing_dispatch.created_at + timedelta(seconds=1),
    )
    database_session.add(example_benchmark_object)
    database_session.add(retry_task)
    database_session.add(newer_retry_task)
    database_session.commit()

    assert ExecutorControlRepository(database_session).record_dispatch_failure(
        benchmark=example_benchmark_object,
        dispatch_id=failing_dispatch.id,
        task_ids=[retry_task.task_id, newer_retry_task.task_id],
        error_message="retry failed",
    )
    database_session.commit()
    database_session.refresh(example_benchmark_object)
    database_session.refresh(failing_dispatch)
    database_session.refresh(sibling_dispatch)
    database_session.refresh(retry_task)
    database_session.refresh(newer_retry_task)

    assert example_benchmark_object.status == BenchmarkStatus.IN_PROGRESS
    assert failing_dispatch.status == ExecutorDispatchStatus.FAILED
    assert sibling_dispatch.status == ExecutorDispatchStatus.RUNNING
    assert retry_task.status == TaskStatus.ERROR
    assert newer_retry_task.status == TaskStatus.PENDING


def test_terminal_recovery_terminalizes_active_dispatches(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    old_release = _release("old")
    active_release = _release("active")
    register_release(database_session, old_release)
    register_release(database_session, active_release)
    promote_release(database_session, old_release.id)
    pin_benchmark_to_release(example_benchmark_object, old_release)
    database_session.add(example_benchmark_object)
    old_dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        old_release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    database_session.add(old_dispatch)
    promote_release(database_session, active_release.id)
    database_session.commit()

    example_benchmark_object.status = BenchmarkStatus.IN_PROGRESS
    example_benchmark_object.finished_at = None
    new_dispatch = admit_recovery_dispatch(
        database_session,
        benchmark=example_benchmark_object,
        pre_action_status=BenchmarkStatus.ERROR,
        dispatch_id=uuid4(),
        kind=ExecutorDispatchKind.RESUME,
        executor_control_repository=ExecutorControlRepository(database_session),
    )
    database_session.commit()
    database_session.refresh(old_dispatch)

    assert example_benchmark_object.current_execution_release_id == active_release.id
    assert new_dispatch.executor_release_id == active_release.id
    assert old_dispatch.status == ExecutorDispatchStatus.FAILED
    assert old_dispatch.finished_at is not None


def test_terminalize_active_dispatches_preserves_selected_finalizer(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("active")
    register_release(database_session, release)
    pin_benchmark_to_release(example_benchmark_object, release)
    database_session.add(example_benchmark_object)
    finalizer = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    sibling = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.RETRY,
        dispatch_id=uuid4(),
    )
    finalizer.status = ExecutorDispatchStatus.RUNNING
    finalizer.started_at = datetime.now(UTC)
    sibling.status = ExecutorDispatchStatus.RUNNING
    sibling.started_at = datetime.now(UTC)
    database_session.add(finalizer)
    database_session.add(sibling)
    database_session.commit()

    ExecutorControlRepository(database_session).terminalize_active_dispatches(
        example_benchmark_object.id,
        except_dispatch_id=finalizer.id,
    )
    database_session.commit()
    database_session.refresh(finalizer)
    database_session.refresh(sibling)

    assert finalizer.status == ExecutorDispatchStatus.RUNNING
    assert sibling.status == ExecutorDispatchStatus.FAILED
    assert sibling.finished_at is not None


@pytest.mark.parametrize(
    ("status", "allowed"),
    [
        (BenchmarkStatus.IN_PROGRESS, True),
        (BenchmarkStatus.FINISHED, True),
        (BenchmarkStatus.ERROR, True),
        (BenchmarkStatus.STOPPING, True),
        (BenchmarkStatus.STOPPED, False),
    ],
)
def test_terminal_authority_rejects_stopped_state(
    status: BenchmarkStatus,
    allowed: bool,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    release = _release("authority")
    register_release(database_session, release)
    promote_release(database_session, release.id)
    pin_benchmark_to_release(example_benchmark_object, release)
    example_benchmark_object.status = status
    if status in (BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR):
        example_benchmark_object.finished_at = datetime.now(UTC)
    dispatch = create_executor_dispatch(
        example_benchmark_object.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    dispatch.status = ExecutorDispatchStatus.RUNNING
    dispatch.started_at = datetime.now(UTC)
    database_session.add(example_benchmark_object)
    database_session.add(dispatch)
    database_session.commit()
    authority = ExecutionAuthority(
        benchmark_id=example_benchmark_object.id,
        dispatch_id=dispatch.id,
    )

    repository = TaskExecutionRepository(database_session)
    if allowed:
        assert (
            repository.lock_execution_authority(authority, require_in_progress=False).id == example_benchmark_object.id
        )
        database_session.rollback()
    else:
        with pytest.raises(ExecutionAuthorityRevoked):
            repository.lock_execution_authority(authority, require_in_progress=False)
        database_session.rollback()
