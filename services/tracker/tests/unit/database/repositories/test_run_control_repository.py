"""Focused tests for database-only run-control persistence.

Run: uv run pytest tests/unit/database/repositories/test_run_control_repository.py
"""

from datetime import datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from sqlmodel import Session, select

from tests.factories import make_benchmark, make_task
from tracker.database.models import Benchmark, BenchmarkStatus, FinalEvaluation, Org, RetryMode, Task, TaskStatus
from tracker.database.repositories import BenchmarkRepository, RetrySelection, RunControlRepository, TaskRepository
from tracker.database.transaction import TrackerTransaction
from tracker.exceptions import TrackerServiceError


def test_run_control_uses_supplied_same_session_repositories(empty_database_session: Session) -> None:
    benchmark_repository = BenchmarkRepository(empty_database_session)
    task_repository = TaskRepository(empty_database_session)
    repository = RunControlRepository(empty_database_session, benchmark_repository, task_repository)

    assert vars(repository)["_benchmarks"] is benchmark_repository
    assert vars(repository)["_tasks"] is task_repository


def test_task_status_counts_scope_benchmark_and_organization(empty_database_session: Session) -> None:
    owner = Org(id=uuid4(), name="owner")
    other_org = Org(id=uuid4(), name="other")
    empty_database_session.add_all([owner, other_org])
    empty_database_session.commit()
    benchmark = make_benchmark(org_id=owner.id, session=empty_database_session)
    other_benchmark = make_benchmark(org_id=other_org.id, session=empty_database_session)
    foreign_task = make_task(benchmark, "foreign", status=TaskStatus.PENDING)
    foreign_task.org_id = other_org.id
    empty_database_session.add_all(
        [
            make_task(benchmark, "pending", status=TaskStatus.PENDING),
            make_task(benchmark, "building", status=TaskStatus.BUILDING),
            make_task(benchmark, "stopped", status=TaskStatus.STOPPED),
            make_task(benchmark, "finished", status=TaskStatus.FINISHED),
            foreign_task,
            make_task(other_benchmark, "other", status=TaskStatus.PENDING),
        ]
    )
    empty_database_session.commit()

    repository = TrackerTransaction.from_session(empty_database_session).run_control

    assert repository.count_runnable_tasks(benchmark.id, owner.id) == 2
    assert repository.count_stopped_tasks(benchmark.id, owner.id) == 1
    assert repository.count_runnable_tasks(benchmark.id, other_org.id) == 0
    assert repository.count_stopped_tasks(benchmark.id, other_org.id) == 0


def test_lock_and_task_mutations_are_organization_scoped(empty_database_session: Session) -> None:
    owner = Org(id=uuid4(), name="owner")
    other = Org(id=uuid4(), name="other")
    empty_database_session.add_all([owner, other])
    empty_database_session.commit()
    benchmark = make_benchmark(org_id=owner.id, session=empty_database_session)
    task = make_task(benchmark, "task", status=TaskStatus.PENDING)
    task.org_id = other.id
    empty_database_session.add(task)
    empty_database_session.commit()
    repository = TrackerTransaction.from_session(empty_database_session).run_control

    assert repository.lock_benchmark(benchmark.id, other.id) is None
    assert repository.stop_active_tasks(benchmark.id, owner.id, task_ids=None) == 0
    empty_database_session.expire_all()
    assert empty_database_session.get(Task, task.id).status == TaskStatus.PENDING  # type: ignore[union-attr]


def test_explicit_empty_task_ids_never_means_whole_run(empty_database_session: Session) -> None:
    benchmark = make_benchmark(session=empty_database_session)
    task = make_task(benchmark, "task", status=TaskStatus.PENDING)
    empty_database_session.add(task)
    empty_database_session.commit()
    repository = TrackerTransaction.from_session(empty_database_session).run_control

    assert repository.apply_stop(benchmark, benchmark.org_id, force=True, task_ids=[]) == 0
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert repository.stop_active_tasks(benchmark.id, benchmark.org_id, task_ids=[]) == 0
    empty_database_session.expire_all()
    assert empty_database_session.get(Task, task.id).status == TaskStatus.PENDING  # type: ignore[union-attr]


def test_stop_writes_remain_uncommitted_until_caller_decides(empty_database_session: Session) -> None:
    benchmark = make_benchmark(session=empty_database_session)
    task = make_task(benchmark, "task", status=TaskStatus.PENDING)
    empty_database_session.add(task)
    empty_database_session.commit()
    repository = TrackerTransaction.from_session(empty_database_session).run_control

    assert repository.apply_stop(benchmark, benchmark.org_id, force=False, task_ids=None) == 1
    assert task.status == TaskStatus.STOPPED
    empty_database_session.rollback()
    empty_database_session.expire_all()
    persisted = empty_database_session.get(Task, task.id)
    assert persisted is not None
    assert persisted.status == TaskStatus.PENDING
    assert empty_database_session.get(Benchmark, benchmark.id).status == BenchmarkStatus.IN_PROGRESS  # type: ignore[union-attr]


def test_retry_selection_is_deterministic_and_preserves_status_matrix(empty_database_session: Session) -> None:
    benchmark = make_benchmark(session=empty_database_session)
    now = datetime(2026, 1, 1, tzinfo=ZoneInfo("UTC"))
    first = make_task(benchmark, "first", status=TaskStatus.ERROR, started_at=now + timedelta(minutes=2))
    second = make_task(benchmark, "second", status=TaskStatus.ERROR, started_at=now)
    stopped = make_task(benchmark, "stopped", status=TaskStatus.STOPPED, started_at=now + timedelta(minutes=1))
    finished = make_task(benchmark, "finished", status=TaskStatus.FINISHED, finished_at=now)
    empty_database_session.add_all([first, second, stopped, finished])
    empty_database_session.commit()
    repository = TrackerTransaction.from_session(empty_database_session).run_control

    active = repository.select_retryable(benchmark, benchmark.org_id, retry=True, rerun_task_ids=["first", "second"])
    assert [task.task_id for task in active.existing_tasks] == ["second", "first"]
    assert active.new_task_ids == []

    benchmark.status = BenchmarkStatus.STOPPED
    empty_database_session.commit()
    terminal = repository.select_retryable(benchmark, benchmark.org_id, retry=False, rerun_task_ids=[])
    assert [task.task_id for task in terminal.existing_tasks] == ["stopped"]
    assert terminal.new_task_ids == []


def test_active_retry_rejects_non_error_ids_without_mutating_rows(empty_database_session: Session) -> None:
    benchmark = make_benchmark(session=empty_database_session)
    task = make_task(benchmark, "stopped", status=TaskStatus.STOPPED)
    empty_database_session.add(task)
    empty_database_session.commit()
    repository = TrackerTransaction.from_session(empty_database_session).run_control

    with pytest.raises(TrackerServiceError, match="cannot be retried"):
        repository.select_retryable(benchmark, benchmark.org_id, retry=True, rerun_task_ids=["stopped"])
    empty_database_session.rollback()
    empty_database_session.expire_all()
    assert empty_database_session.get(Task, task.id).status == TaskStatus.STOPPED  # type: ignore[union-attr]


def test_failed_retry_precondition_leaves_database_state_unchanged(empty_database_session: Session) -> None:
    benchmark = make_benchmark(session=empty_database_session)
    task = make_task(benchmark, "task", status=TaskStatus.STOPPED)
    evaluation = FinalEvaluation(org_id=benchmark.org_id, benchmark=benchmark.id, final_score=1, properties={})
    other_org = Org(id=uuid4(), name="other")
    empty_database_session.add_all([task, evaluation, other_org])
    empty_database_session.commit()
    repository = TrackerTransaction.from_session(empty_database_session).run_control
    selection = RetrySelection(benchmark, [task], [])

    with pytest.raises(TrackerServiceError, match="does not belong"):
        repository.apply_retry(
            selection,
            other_org.id,
            retry_mode=RetryMode.AUTO,
        )
    empty_database_session.rollback()
    empty_database_session.expire_all()
    assert empty_database_session.get(Task, task.id).status == TaskStatus.STOPPED  # type: ignore[union-attr]
    assert empty_database_session.get(FinalEvaluation, evaluation.id) is not None
    assert empty_database_session.get(Benchmark, benchmark.id).status == BenchmarkStatus.IN_PROGRESS  # type: ignore[union-attr]


def test_apply_retry_deletes_only_owned_final_evaluation_and_creates_missing_rows(
    empty_database_session: Session,
) -> None:
    benchmark = make_benchmark(session=empty_database_session)
    other = Org(id=uuid4(), name="other")
    empty_database_session.add(other)
    existing = make_task(benchmark, "existing", status=TaskStatus.STOPPED)
    empty_database_session.add(existing)
    owner_evaluation = FinalEvaluation(org_id=benchmark.org_id, benchmark=benchmark.id, final_score=1, properties={})
    foreign_evaluation = FinalEvaluation(org_id=other.id, benchmark=benchmark.id, final_score=2, properties={})
    empty_database_session.add_all([owner_evaluation, foreign_evaluation])
    empty_database_session.commit()
    repository = TrackerTransaction.from_session(empty_database_session).run_control
    selection = RetrySelection(benchmark, [existing], ["missing"])

    repository.apply_retry(
        selection,
        benchmark.org_id,
        retry_mode=RetryMode.FROM_SCRATCH,
    )
    assert (
        empty_database_session.exec(select(FinalEvaluation).where(FinalEvaluation.org_id == benchmark.org_id)).all()
        == []
    )
    assert empty_database_session.get(FinalEvaluation, foreign_evaluation.id) is not None
    created = empty_database_session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.task_id == "missing")
    ).one()
    assert created.org_id == benchmark.org_id
    assert created.status == TaskStatus.PENDING
    assert existing.status == TaskStatus.PENDING
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS

    empty_database_session.rollback()
    assert empty_database_session.get(FinalEvaluation, owner_evaluation.id) is not None
    assert empty_database_session.exec(select(Task).where(Task.task_id == "missing")).all() == []


def test_apply_retry_clears_loaded_owned_final_evaluation(empty_database_session: Session) -> None:
    benchmark = make_benchmark(session=empty_database_session)
    task = make_task(benchmark, "task", status=TaskStatus.STOPPED)
    evaluation = FinalEvaluation(org_id=benchmark.org_id, benchmark=benchmark.id, final_score=1, properties={})
    empty_database_session.add_all([task, evaluation])
    empty_database_session.commit()
    assert benchmark.final_evaluation is not None

    TrackerTransaction.from_session(empty_database_session).run_control.apply_retry(
        RetrySelection(benchmark, [task], []),
        benchmark.org_id,
        retry_mode=RetryMode.FROM_SCRATCH,
    )

    assert benchmark.final_evaluation is None
