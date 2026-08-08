"""Focused tests for executor-dispatch persistence."""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tracker.database.models import (
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    TaskStatus,
)
from tracker.database.repositories import EnqueueFailureResolution, ExecutorControlRepository


def _dispatch(
    benchmark_id: UUID,
    *,
    status: ExecutorDispatchStatus,
    created_at: datetime,
) -> ExecutorDispatch:
    return ExecutorDispatch(
        id=uuid4(),
        benchmark_id=benchmark_id,
        kind=ExecutorDispatchKind.START,
        status=status,
        executor_release_id="release",
        executor_artifact_uri="s3://release/executor.pex",
        executor_artifact_digest="a" * 64,
        executor_protocol_version="1",
        created_at=created_at,
    )


def test_terminalize_preserves_selected_dispatch_and_only_active_statuses(empty_database_session: Session) -> None:
    benchmark = make_benchmark(session=empty_database_session)
    now = datetime.now(UTC)
    selected = _dispatch(benchmark.id, status=ExecutorDispatchStatus.RUNNING, created_at=now)
    queued = _dispatch(benchmark.id, status=ExecutorDispatchStatus.QUEUED, created_at=now)
    finished = _dispatch(benchmark.id, status=ExecutorDispatchStatus.FINISHED, created_at=now)
    empty_database_session.add_all([selected, queued, finished])
    empty_database_session.commit()

    ExecutorControlRepository(empty_database_session).terminalize_active_dispatches(
        benchmark.id,
        except_dispatch_id=selected.id,
    )
    empty_database_session.commit()
    empty_database_session.refresh(selected)
    empty_database_session.refresh(queued)
    empty_database_session.refresh(finished)

    assert selected.status == ExecutorDispatchStatus.RUNNING
    assert queued.status == ExecutorDispatchStatus.FAILED
    assert finished.status == ExecutorDispatchStatus.FINISHED


def test_record_failure_scopes_tasks_to_benchmark_owner(empty_database_session: Session) -> None:
    benchmark = make_benchmark(session=empty_database_session)
    foreign_benchmark = make_benchmark(name="foreign", org_id=uuid4(), session=empty_database_session)
    created_at = datetime.now(UTC)
    dispatch = _dispatch(benchmark.id, status=ExecutorDispatchStatus.RUNNING, created_at=created_at)
    owner_task = make_task(benchmark, "owner", status=TaskStatus.PENDING, started_at=created_at - timedelta(seconds=1))
    foreign_task = make_task(
        benchmark,
        "foreign",
        status=TaskStatus.PENDING,
        started_at=created_at - timedelta(seconds=1),
    )
    foreign_task.org_id = foreign_benchmark.org_id
    empty_database_session.add_all([dispatch, owner_task, foreign_task])
    empty_database_session.commit()

    assert ExecutorControlRepository(empty_database_session).record_dispatch_failure(
        benchmark=benchmark,
        dispatch_id=dispatch.id,
        task_ids=[owner_task.task_id, foreign_task.task_id],
        error_message="dispatch failed",
    )
    empty_database_session.commit()
    empty_database_session.refresh(owner_task)
    empty_database_session.refresh(foreign_task)

    assert owner_task.status == TaskStatus.ERROR
    assert foreign_task.status == TaskStatus.PENDING
    assert benchmark.status == BenchmarkStatus.ERROR


def test_enqueue_failure_stages_changes_without_committing(empty_database_session: Session) -> None:
    benchmark = make_benchmark(session=empty_database_session)
    created_at = datetime.now(UTC)
    dispatch = _dispatch(benchmark.id, status=ExecutorDispatchStatus.QUEUED, created_at=created_at)
    task = make_task(benchmark, "task", status=TaskStatus.PENDING, started_at=created_at - timedelta(seconds=1))
    empty_database_session.add_all([dispatch, task])
    empty_database_session.commit()

    resolution = ExecutorControlRepository(empty_database_session).resolve_enqueue_failure(
        benchmark_id=benchmark.id,
        dispatch_id=dispatch.id,
        task_ids=[task.task_id],
    )
    assert resolution == EnqueueFailureResolution.FAILED

    empty_database_session.rollback()
    empty_database_session.refresh(benchmark)
    empty_database_session.refresh(dispatch)
    empty_database_session.refresh(task)
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert dispatch.status == ExecutorDispatchStatus.QUEUED
    assert task.status == TaskStatus.PENDING


def test_enqueue_failure_reports_delivered_and_superseded_without_mutation(empty_database_session: Session) -> None:
    benchmark = make_benchmark(session=empty_database_session)
    delivered = _dispatch(benchmark.id, status=ExecutorDispatchStatus.RUNNING, created_at=datetime.now(UTC))
    delivered.started_at = datetime.now(UTC)
    superseded = _dispatch(benchmark.id, status=ExecutorDispatchStatus.FAILED, created_at=datetime.now(UTC))
    empty_database_session.add_all([delivered, superseded])
    empty_database_session.commit()

    repository = ExecutorControlRepository(empty_database_session)
    assert (
        repository.resolve_enqueue_failure(
            benchmark_id=benchmark.id,
            dispatch_id=delivered.id,
            task_ids=[],
        )
        == EnqueueFailureResolution.DELIVERED
    )
    assert (
        repository.resolve_enqueue_failure(
            benchmark_id=benchmark.id,
            dispatch_id=superseded.id,
            task_ids=[],
        )
        == EnqueueFailureResolution.SUPERSEDED
    )
