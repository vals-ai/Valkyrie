import asyncio
from datetime import datetime, timedelta
from typing import Any, Literal
from unittest.mock import AsyncMock, Mock
from zoneinfo import ZoneInfo

import pytest
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import FinalScoreResponse, VerifyTaskIdsResponse
from sqlmodel import Session, select

from tests.conftest import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    Org,
    RetryMode,
    Task,
    TaskStatus,
)
from tracker.types import HarnessConfig
from tracker.utils import (
    catch_errors_during_cleanup,
    commit_task_error,
    commit_task_status_transition,
    process_benchmark,
    reset_to_in_progress_status,
    save_eval_resume_state,
    TaskMonitor,
    TrackedTask,
    TrackedTaskStatus,
)
from tracker.utils.run_orchestration import TaskBatch

UTC = ZoneInfo("UTC")
OLD_ATTEMPT = datetime(2026, 7, 9, 1, tzinfo=UTC)
NEW_ATTEMPT = OLD_ATTEMPT + timedelta(seconds=1)
TEST_ORG = Org(id=TEST_ORG_ID, name="default")


def _task_batch(kind: Literal["start", "retry"], task_id: str, started_at: datetime) -> TaskBatch:
    return {"kind": kind, "attempts": {task_id: started_at.isoformat()}}


def _create_task(
    database_session: Session,
    contract: AgentContractRequest,
    *,
    status: TaskStatus = TaskStatus.PENDING,
) -> Task:
    benchmark = Benchmark(
        org_id=TEST_ORG_ID,
        name="swebench",
        status=BenchmarkStatus.IN_PROGRESS,
        arguments=BenchmarkArguments(contract=contract, concurrency=1),
    )
    database_session.add(benchmark)
    database_session.commit()
    task = Task(
        org_id=TEST_ORG_ID,
        task_id="task_0",
        benchmark=benchmark.id,
        status=status,
        started_at=OLD_ATTEMPT,
    )
    database_session.add(task)
    database_session.commit()
    return task


def test_stale_attempt_writes_are_rejected(
    database_session: Session,
    contract: AgentContractRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _create_task(database_session, contract)
    task.started_at = NEW_ATTEMPT
    task.eval_resume_state = {"attempt": "new"}
    database_session.add(task)
    database_session.commit()
    monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)

    assert not commit_task_error(task, database_session, OLD_ATTEMPT, "old attempt failed")
    assert not commit_task_status_transition(task.id, database_session, TEST_ORG, OLD_ATTEMPT, TaskStatus.BUILDING)
    save_eval_resume_state(task.id, TEST_ORG, OLD_ATTEMPT, {"attempt": "old"})

    task.status = TaskStatus.STOPPED
    database_session.add(task)
    database_session.commit()
    assert not commit_task_error(task, database_session, NEW_ATTEMPT, "late failure")
    assert not commit_task_status_transition(task.id, database_session, TEST_ORG, NEW_ATTEMPT, TaskStatus.FINISHED)

    database_session.refresh(task)
    assert task.status == TaskStatus.STOPPED
    assert task.eval_resume_state == {"attempt": "new"}
    assert database_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).all() == []


def test_stale_cleanup_only_errors_owned_current_attempts(
    database_session: Session,
    contract: AgentContractRequest,
) -> None:
    retried_task = _create_task(database_session, contract)
    owned_task = Task(
        org_id=TEST_ORG_ID,
        task_id="task_1",
        benchmark=retried_task.benchmark,
        started_at=OLD_ATTEMPT,
    )
    database_session.add(owned_task)
    database_session.commit()
    retried_task.started_at = NEW_ATTEMPT
    retried_task.status = TaskStatus.FINISHED
    database_session.add(retried_task)
    database_session.commit()

    cleanup_owned = catch_errors_during_cleanup(
        retried_task.benchmark,
        database_session,
        TEST_ORG,
        {retried_task.task_id: OLD_ATTEMPT, owned_task.task_id: OLD_ATTEMPT},
        {retried_task.task_id: OLD_ATTEMPT, owned_task.task_id: OLD_ATTEMPT},
    )

    database_session.refresh(retried_task)
    database_session.refresh(owned_task)
    benchmark = database_session.get(Benchmark, retried_task.benchmark)
    assert benchmark is not None
    assert not cleanup_owned
    assert retried_task.status == TaskStatus.FINISHED
    assert owned_task.status == TaskStatus.ERROR
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert database_session.exec(select(ErrorResult.task)).all() == [owned_task.id]


async def test_resume_commits_status_and_attempt_generation_atomically(
    database_session: Session,
    contract: AgentContractRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _create_task(database_session, contract, status=TaskStatus.STOPPED)
    benchmark = database_session.get(Benchmark, task.benchmark)
    assert benchmark is not None
    benchmark.status = BenchmarkStatus.STOPPED
    database_session.add(benchmark)
    database_session.commit()
    benchmark_service = benchmark.benchmark_service()
    benchmark_service.verify_task_ids = AsyncMock(return_value=VerifyTaskIdsResponse(task_ids=[task.task_id]))
    commits: list[tuple[BenchmarkStatus, TaskStatus, datetime]] = []
    commit = database_session.commit

    def record_commit() -> None:
        commit()
        commits.append((benchmark.status, task.status, task.started_at))

    monkeypatch.setattr(database_session, "commit", record_commit)

    verified_task_ids = await reset_to_in_progress_status(
        benchmark,
        database_session,
        benchmark_service,
        False,
        RetryMode.AUTO,
        [],
        TEST_ORG,
    )

    assert list(verified_task_ids) == [task.task_id]
    assert len(commits) == 1
    assert commits[0][0:2] == (BenchmarkStatus.IN_PROGRESS, TaskStatus.PENDING)
    assert commits[0][2] != OLD_ATTEMPT
    await benchmark_service.close()


async def test_stale_worker_exception_does_not_error_retry(
    database_session: Session,
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _create_task(database_session, contract)
    benchmark = database_session.get(Benchmark, task.benchmark)
    assert benchmark is not None

    async def retry_then_fail(*_args: Any, **_kwargs: Any) -> None:
        with Session(database_session.bind) as session:
            current_task = session.get(Task, task.id)
            assert current_task is not None
            current_task.started_at = NEW_ATTEMPT
            current_task.status = TaskStatus.PENDING
            session.add(current_task)
            session.commit()
        raise RuntimeError("old worker failed")

    monkeypatch.setattr("tracker.utils.run_orchestration.copy_agent_to_benchmark", retry_then_fail)
    await process_benchmark(
        benchmark.start_benchmark_request(harness_config).model_dump(),
        str(benchmark.id),
        [task.task_id],
    )

    database_session.refresh(task)
    database_session.refresh(benchmark)
    assert task.status == TaskStatus.PENDING
    assert task.started_at.replace(tzinfo=UTC) == NEW_ATTEMPT
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert benchmark.error_message is None
    assert database_session.exec(select(ErrorResult)).all() == []


@pytest.mark.parametrize("kind", ["start", "retry"])
async def test_delayed_message_cannot_adopt_new_attempt(
    kind: Literal["start", "retry"],
    database_session: Session,
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _create_task(database_session, contract)
    benchmark = database_session.get(Benchmark, task.benchmark)
    assert benchmark is not None
    task.started_at = NEW_ATTEMPT
    database_session.add(task)
    database_session.commit()
    process_task = AsyncMock()
    monkeypatch.setattr("tracker.utils.run_orchestration.process_task", process_task)

    await process_benchmark(
        benchmark.start_benchmark_request(harness_config).model_dump(),
        str(benchmark.id),
        _task_batch(kind, task.task_id, OLD_ATTEMPT),
    )

    database_session.refresh(task)
    database_session.refresh(benchmark)
    process_task.assert_not_called()
    assert task.status == TaskStatus.PENDING
    assert task.started_at.replace(tzinfo=UTC) == NEW_ATTEMPT
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS


async def test_monitor_cancels_superseded_attempt(
    database_session: Session,
    contract: AgentContractRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _create_task(database_session, contract)
    task.started_at = NEW_ATTEMPT
    database_session.add(task)
    database_session.commit()
    monkeypatch.setattr("tracker.utils.task_execution.engine", database_session.bind)
    tracked_task = TrackedTask(asyncio.sleep(0), TEST_ORG, OLD_ATTEMPT)
    tracked_task._status = TrackedTaskStatus.WAITING  # type: ignore[attr-defined]

    def cancel(*_args: Any) -> None:
        tracked_task._status = TrackedTaskStatus.DONE  # type: ignore[attr-defined]

    cancel_mock = Mock(side_effect=cancel)
    tracked_task._task = Mock(cancel=cancel_mock, done=lambda: False)  # type: ignore[assignment]
    monitor = TaskMonitor(task.benchmark, {task.task_id: tracked_task}, TEST_ORG)
    monitor._TRACK_INTERVAL = 0  # pyright: ignore[reportPrivateUsage]

    await monitor.track_tasks()

    cancel_mock.assert_called_once()
    tracked_task._coro.close()  # pyright: ignore[reportPrivateUsage]


async def test_terminal_redelivery_finalizes_without_rerunning_task(
    database_session: Session,
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _create_task(database_session, contract, status=TaskStatus.FINISHED)
    database_session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, result={"score": 1.0}))
    database_session.commit()
    benchmark = database_session.get(Benchmark, task.benchmark)
    assert benchmark is not None
    process_task = AsyncMock(side_effect=AssertionError("terminal task must not execute again"))
    monkeypatch.setattr("tracker.utils.run_orchestration.process_task", process_task)

    await process_benchmark(
        benchmark.start_benchmark_request(harness_config).model_dump(),
        str(benchmark.id),
        _task_batch("retry", task.task_id, task.started_at),
    )

    database_session.refresh(benchmark)
    process_task.assert_not_called()
    assert benchmark.status == BenchmarkStatus.FINISHED
    assert benchmark.final_evaluation is not None


async def test_final_score_revalidates_attempts_before_publication(
    database_session: Session,
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _create_task(database_session, contract, status=TaskStatus.FINISHED)
    database_session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, result={"score": 1.0}))
    database_session.commit()
    benchmark = database_session.get(Benchmark, task.benchmark)
    assert benchmark is not None

    async def retry_during_score(*_args: Any, **_kwargs: Any) -> FinalScoreResponse:
        with Session(database_session.bind) as session:
            current_task = session.get(Task, task.id)
            assert current_task is not None
            current_task.started_at = NEW_ATTEMPT
            current_task.status = TaskStatus.PENDING
            session.add(current_task)
            session.commit()
        return FinalScoreResponse(tasks_evaluated=[task.task_id], final_score=1.0, metadata={})

    monkeypatch.setattr(BenchmarkServiceClient, "final_score", retry_during_score)
    await process_benchmark(
        benchmark.start_benchmark_request(harness_config).model_dump(),
        str(benchmark.id),
        [task.task_id],
    )

    database_session.refresh(benchmark)
    database_session.refresh(task)
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert benchmark.final_evaluation is None
    assert task.status == TaskStatus.PENDING
    assert task.started_at.replace(tzinfo=UTC) == NEW_ATTEMPT


async def test_late_lambda_error_does_not_poison_retry(
    database_session: Session,
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _create_task(database_session, contract, status=TaskStatus.FINISHED)
    database_session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, result={"score": 1.0}))
    benchmark = database_session.get(Benchmark, task.benchmark)
    assert benchmark is not None
    benchmark.arguments = benchmark.arguments.model_copy(update={"lambda_function": "final-view"})
    database_session.add(benchmark)
    database_session.commit()

    def retry_then_fail(*_args: Any, **_kwargs: Any) -> None:
        with Session(database_session.bind) as session:
            current_benchmark = session.get(Benchmark, benchmark.id)
            assert current_benchmark is not None
            current_benchmark.status = BenchmarkStatus.IN_PROGRESS
            new_task = Task(
                org_id=TEST_ORG_ID,
                task_id="task_1",
                benchmark=benchmark.id,
                status=TaskStatus.FINISHED,
                started_at=NEW_ATTEMPT,
            )
            session.add(current_benchmark)
            session.add(new_task)
            session.flush()
            session.add(EvaluationResult(org_id=TEST_ORG_ID, task=new_task.id, result={"score": 1.0}))
            session.commit()
        raise TimeoutError("old Lambda timed out")

    monkeypatch.setattr("tracker.utils.run_orchestration.invoke_lambda", retry_then_fail)
    await process_benchmark(
        benchmark.start_benchmark_request(harness_config).model_dump(),
        str(benchmark.id),
        [task.task_id],
    )

    database_session.refresh(benchmark)
    database_session.refresh(task)
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert benchmark.error_message is None
    assert task.status == TaskStatus.FINISHED
    added_task = database_session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.task_id == "task_1")
    ).one()
    assert added_task.status == TaskStatus.FINISHED
    assert database_session.exec(select(ErrorResult)).all() == []


async def test_terminal_redelivery_does_not_reopen_run(
    database_session: Session,
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    process_benchmark_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = _create_task(database_session, contract, status=TaskStatus.FINISHED)
    database_session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, result={"score": 1.0}))
    benchmark = database_session.get(Benchmark, task.benchmark)
    assert benchmark is not None
    benchmark.status = BenchmarkStatus.FINISHED
    database_session.add(benchmark)
    database_session.commit()
    score = AsyncMock()
    monkeypatch.setattr(BenchmarkServiceClient, "final_score", score)

    await process_benchmark(
        benchmark.start_benchmark_request(harness_config).model_dump(),
        str(benchmark.id),
        [task.task_id],
    )

    database_session.refresh(benchmark)
    score.assert_not_called()
    assert benchmark.status == BenchmarkStatus.FINISHED
