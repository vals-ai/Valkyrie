"""Tests for direct and PostgreSQL-queued run coordination.

Run: uv run pytest tests/unit/utils/test_run_orchestration.py
"""

import asyncio
from contextlib import suppress
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from benchmark_service import SandboxProvider
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import VerifyTaskIdsResponse
from sqlmodel import Session, select

from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
    Org,
    RetryMode,
    Task,
    TaskStatus,
)
from tracker.scheduler.store import queue_pool_id
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import process_benchmark, reset_to_in_progress_status, start_benchmark_request_to_benchmark


def _persist_benchmark(
    request: StartBenchmarkRequest,
    session: Session,
    *,
    queued_pool_id: str | None = None,
) -> Benchmark:
    org = Org(id=TEST_ORG_ID, name="default")
    benchmark = start_benchmark_request_to_benchmark(
        request,
        RequestIdentity(org=org, access_key_id=None, email=None, name=None),
        queue_pool_id=queued_pool_id,
    )
    session.add(benchmark)
    session.commit()
    return benchmark


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_coordinator_limits_evaluations_and_pending_contenders(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_ids = ["evaluation_0", "evaluation_1", "evaluation_2", "pending_0", "pending_1"]
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        concurrency=2,
        priority=3,
        task_ids=task_ids,
        harness_config=harness_config,
    )
    provider_pool_id = "coordinator-pool"
    benchmark = _persist_benchmark(
        request,
        database_session,
        queued_pool_id=queue_pool_id(provider_pool_id),
    )
    database_session.add_all(
        [
            Task(
                org_id=TEST_ORG_ID,
                task_id=task_id,
                benchmark=benchmark.id,
                status=TaskStatus.EVALUATING,
                eval_resume_state={"cursor": task_id},
            )
            for task_id in task_ids[:3]
        ]
        + [
            Task(
                org_id=TEST_ORG_ID,
                task_id=task_id,
                benchmark=benchmark.id,
                status=TaskStatus.EVALUATING,
            )
            for task_id in task_ids[3:]
        ]
    )
    database_session.commit()
    evaluation_started: list[str] = []
    evaluation_finished = {task_id: asyncio.Event() for task_id in task_ids[:3]}
    release_evaluation = {task_id: asyncio.Event() for task_id in task_ids[:3]}
    two_evaluations_started = asyncio.Event()
    third_evaluation_started = asyncio.Event()
    pending_started: list[str] = []
    pending_contender_started = asyncio.Event()
    hold_pending = asyncio.Event()
    coordinator_polls: asyncio.Queue[None] = asyncio.Queue()
    continue_coordinator: asyncio.Queue[None] = asyncio.Queue()
    original_sleep = asyncio.sleep

    def set_concurrency(value: int) -> None:
        with Session(bind=database_session.bind) as session:
            persisted_benchmark = session.get(Benchmark, benchmark.id)
            assert persisted_benchmark is not None
            persisted_benchmark.arguments = persisted_benchmark.arguments.model_copy(update={"concurrency": value})
            session.add(persisted_benchmark)
            session.commit()

    async def process_task(
        task_row: Task,
        *_args: Any,
        **_kwargs: Any,
    ) -> dict[str, dict[str, Any] | None]:
        if task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is not None:
            evaluation_started.append(task_row.task_id)
            if len(evaluation_started) == 2:
                two_evaluations_started.set()
            if len(evaluation_started) == 3:
                third_evaluation_started.set()
            await release_evaluation[task_row.task_id].wait()
            result = {"status": "success", "score": 1.0}
            with Session(bind=database_session.bind) as session:
                task = session.get(Task, task_row.id)
                assert task is not None
                task.status = TaskStatus.FINISHED
                session.add(task)
                session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, instance_id=None, result=result))
                session.commit()
            evaluation_finished[task_row.task_id].set()

            return {task_row.task_id: result}

        pending_started.append(task_row.task_id)
        pending_contender_started.set()
        await hold_pending.wait()

        return {task_row.task_id: None}

    async def controlled_sleep(seconds: float) -> None:
        if seconds != 1.0:
            await original_sleep(0)

            return
        coordinator_polls.put_nowait(None)
        await continue_coordinator.get()

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    monkeypatch.setattr("tracker.utils.run_orchestration.process_task", process_task)
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", Mock(return_value=sandbox_provider))
    monkeypatch.setattr("tracker.utils.run_orchestration.asyncio.sleep", controlled_sleep)
    run = asyncio.create_task(process_benchmark(request.model_dump(), str(benchmark.id), task_ids))
    try:
        await coordinator_polls.get()
        await two_evaluations_started.wait()

        assert len(evaluation_started) == 2
        assert pending_started == []

        set_concurrency(1)
        first_evaluation = evaluation_started[0]
        release_evaluation[first_evaluation].set()
        await evaluation_finished[first_evaluation].wait()
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()

        assert len(evaluation_started) == 2
        assert pending_started == []

        set_concurrency(2)
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()
        await third_evaluation_started.wait()

        assert len(evaluation_started) == 3
        assert pending_started == []

        for task_id in evaluation_started[1:]:
            release_evaluation[task_id].set()
            await evaluation_finished[task_id].wait()
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()
        await pending_contender_started.wait()

        assert len(pending_started) == 1

        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()

        assert len(pending_started) == 1
    finally:
        run.cancel()
        with suppress(asyncio.CancelledError):
            await run


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_process_benchmark_finishes_evaluation_resume_before_finalizing(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_ids = ["resume_eval"]
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        concurrency=1,
        priority=3,
        task_ids=task_ids,
        harness_config=harness_config,
    )
    provider_pool_id = "evaluation-pool"
    benchmark = _persist_benchmark(
        request,
        database_session,
        queued_pool_id=queue_pool_id(provider_pool_id),
    )
    database_session.add(
        Task(
            org_id=TEST_ORG_ID,
            task_id="resume_eval",
            benchmark=benchmark.id,
            status=TaskStatus.EVALUATING,
            eval_resume_state={"cursor": "durable"},
        )
    )
    database_session.commit()
    initial_statuses: dict[str, TaskStatus] = {}

    async def process_task(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
        initial_statuses[task_row.task_id] = task_row.status
        result = {"status": "success", "score": 1.0}
        with Session(bind=database_session.bind) as session:
            task = session.get(Task, task_row.id)
            assert task is not None
            task.status = TaskStatus.FINISHED
            session.add(task)
            session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, instance_id=None, result=result))
            session.commit()

        return {task_row.task_id: result}

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    monkeypatch.setattr("tracker.utils.run_orchestration.process_task", process_task)
    monkeypatch.setattr(
        BenchmarkServiceClient,
        "get_sandbox_provider",
        Mock(return_value=sandbox_provider),
    )

    await process_benchmark(request.model_dump(), str(benchmark.id), task_ids)

    database_session.refresh(benchmark)
    assert initial_statuses == {"resume_eval": TaskStatus.EVALUATING}
    assert benchmark.status == BenchmarkStatus.FINISHED


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_building_only_run_reaches_startup_recovery(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_ids = ["task_0"]
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        priority=3,
        task_ids=task_ids,
        harness_config=harness_config,
    )
    provider_pool_id = "recovery-pool"
    benchmark = _persist_benchmark(
        request,
        database_session,
        queued_pool_id=queue_pool_id(provider_pool_id),
    )
    database_session.add(
        Task(
            org_id=TEST_ORG_ID,
            task_id=task_ids[0],
            benchmark=benchmark.id,
            status=TaskStatus.BUILDING,
        )
    )
    database_session.commit()
    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    run_queued_tasks = AsyncMock()
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", Mock(return_value=sandbox_provider))
    monkeypatch.setattr("tracker.utils.run_orchestration._run_queued_tasks", run_queued_tasks)

    await process_benchmark(request.model_dump(), str(benchmark.id), task_ids)

    queued_call = run_queued_tasks.await_args
    assert queued_call is not None
    queued_task_rows = queued_call.kwargs["task_rows"]
    assert [(task_id, task.status) for task_id, task in queued_task_rows] == [
        ("task_0", TaskStatus.BUILDING),
    ]


@pytest.mark.usefixtures("process_benchmark_env")
async def test_process_benchmark_defaults_to_five_concurrent_tasks(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_ids = [f"task_{index}" for index in range(7)]
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        task_ids=task_ids,
        harness_config=harness_config,
    )
    benchmark = _persist_benchmark(request, database_session)
    five_started = asyncio.Event()
    release = asyncio.Event()
    started: list[str] = []

    async def process_task(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
        started.append(task_row.task_id)
        if len(started) == 5:
            five_started.set()
        await release.wait()
        result = {"status": "success", "score": 1.0}
        with Session(bind=database_session.bind) as session:
            task = session.get(Task, task_row.id)
            assert task is not None
            task.status = TaskStatus.FINISHED
            session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, instance_id=task.task_id, result=result))
            session.commit()
        return {task_row.task_id: result}

    monkeypatch.setattr("tracker.utils.run_orchestration.process_task", process_task)
    run = asyncio.create_task(
        process_benchmark(
            request.model_dump(exclude={"concurrency"}),
            str(benchmark.id),
            task_ids,
        )
    )

    await asyncio.wait_for(five_started.wait(), timeout=2)
    await asyncio.sleep(0)
    assert len(started) == 5

    release.set()
    await asyncio.wait_for(run, timeout=2)
    assert len(started) == 7


@pytest.mark.usefixtures("process_benchmark_env")
async def test_process_benchmark_handles_provider_config_failure(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_ids = ["task_0"]
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        task_ids=task_ids,
        harness_config=harness_config,
    )
    benchmark = _persist_benchmark(request, database_session)
    monkeypatch.setattr(
        "tracker.utils.run_orchestration.fetch_sandbox_provider_config",
        Mock(side_effect=RuntimeError("provider secret drift")),
    )
    provider_warning = Mock()
    monkeypatch.setattr("tracker.utils.run_orchestration.logger.warning", provider_warning)
    fallback_cleanup = Mock()
    monkeypatch.setattr("tracker.utils.run_orchestration.catch_errors_during_cleanup", fallback_cleanup)

    await process_benchmark(request.model_dump(), str(benchmark.id), task_ids)

    database_session.refresh(benchmark)
    tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
    assert benchmark.status == BenchmarkStatus.ERROR
    assert benchmark.error_message == "Sandbox provider configuration is unavailable"
    assert "provider secret drift" not in (benchmark.error_message or "")
    assert [task.status for task in tasks] == [TaskStatus.ERROR]
    provider_warning.assert_called_once_with(
        "Sandbox provider configuration resolution failed (%s)",
        "RuntimeError",
    )
    fallback_cleanup.assert_not_called()

    benchmark_service = AsyncMock(spec=BenchmarkServiceClient)
    benchmark_service.verify_task_ids.return_value = VerifyTaskIdsResponse(task_ids=task_ids)
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    assert (
        await reset_to_in_progress_status(
            benchmark_row=benchmark,
            session=database_session,
            benchmark_service=benchmark_service,
            retry=True,
            retry_mode=RetryMode.AUTO,
            rerun_task_ids=[],
            org=org,
        )
        == task_ids
    )

    database_session.refresh(benchmark)
    database_session.refresh(tasks[0])
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert benchmark.error_message is None
    assert benchmark.finished_at is None
    assert tasks[0].status == TaskStatus.PENDING


@pytest.mark.usefixtures("process_benchmark_env")
async def test_process_benchmark_cancellation_only_fails_its_tasks(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_ids = ["task_0"]
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        task_ids=task_ids,
        harness_config=harness_config,
    )
    benchmark = _persist_benchmark(request, database_session)
    unrelated_task = Task(
        org_id=TEST_ORG_ID,
        task_id="task_unrelated",
        benchmark=benchmark.id,
        status=TaskStatus.PENDING,
    )
    database_session.add(unrelated_task)
    database_session.commit()
    monkeypatch.setattr(
        "tracker.utils.run_orchestration.fetch_sandbox_provider_config",
        Mock(side_effect=asyncio.CancelledError),
    )

    with pytest.raises(asyncio.CancelledError):
        await process_benchmark(request.model_dump(), str(benchmark.id), task_ids)

    database_session.refresh(benchmark)
    tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert {task.task_id: task.status for task in tasks} == {
        "task_unrelated": TaskStatus.PENDING,
        "task_0": TaskStatus.ERROR,
    }


@pytest.mark.usefixtures("process_benchmark_env")
@pytest.mark.parametrize("provider_pool_id", [None, "different-pool"])
async def test_queued_process_benchmark_reports_provider_configuration_drift(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
    provider_pool_id: str | None,
) -> None:
    task_ids = ["task_0", "task_1"]
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        priority=3,
        task_ids=task_ids,
        harness_config=harness_config,
    )
    benchmark = _persist_benchmark(
        request,
        database_session,
        queued_pool_id=queue_pool_id("expected-pool"),
    )
    active_task = Task(
        org_id=TEST_ORG_ID,
        task_id="task_0",
        benchmark=benchmark.id,
        status=TaskStatus.BUILDING,
    )
    unrelated_task = Task(
        org_id=TEST_ORG_ID,
        task_id="task_unrelated",
        benchmark=benchmark.id,
        status=TaskStatus.PENDING,
    )
    database_session.add_all([active_task, unrelated_task])
    database_session.commit()
    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", Mock(return_value=sandbox_provider))

    await process_benchmark(request.model_dump(), str(benchmark.id), task_ids)

    database_session.refresh(benchmark)
    tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
    task_statuses = {task.task_id: task.status for task in tasks}
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert benchmark.error_message is None
    assert task_statuses == {
        "task_unrelated": TaskStatus.PENDING,
        "task_0": TaskStatus.BUILDING,
        "task_1": TaskStatus.ERROR,
    }
    benchmark_service = AsyncMock(spec=BenchmarkServiceClient)
    benchmark_service.verify_task_ids.return_value = VerifyTaskIdsResponse(task_ids=task_ids)
    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None

    verified_task_ids = await reset_to_in_progress_status(
        benchmark_row=benchmark,
        session=database_session,
        benchmark_service=benchmark_service,
        retry=True,
        retry_mode=RetryMode.AUTO,
        rerun_task_ids=[],
        org=org,
    )

    database_session.refresh(benchmark)
    for task in tasks:
        database_session.refresh(task)
    assert verified_task_ids == task_ids
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert {task.task_id: task.status for task in tasks} == {
        "task_unrelated": TaskStatus.PENDING,
        "task_0": TaskStatus.BUILDING,
        "task_1": TaskStatus.PENDING,
    }


@pytest.mark.usefixtures("process_benchmark_env")
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_queued_process_benchmark_always_closes_clients(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
    cleanup_fails: bool,
) -> None:
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        priority=3,
        task_ids=[],
        harness_config=harness_config,
    )
    benchmark = _persist_benchmark(
        request,
        database_session,
        queued_pool_id=queue_pool_id("shared-pool"),
    )

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id="shared-pool")
    failure = RuntimeError("run cleanup failed" if cleanup_fails else "benchmark service close failed")
    close_benchmark_service = AsyncMock(side_effect=None if cleanup_fails else failure)
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", Mock(return_value=sandbox_provider))
    monkeypatch.setattr(BenchmarkServiceClient, "close", close_benchmark_service)
    monkeypatch.setattr(
        "tracker.utils.run_orchestration.catch_errors_during_cleanup",
        Mock(side_effect=failure if cleanup_fails else None),
    )

    with pytest.raises(RuntimeError, match=str(failure)):
        await process_benchmark(request.model_dump(), str(benchmark.id), [])

    close_benchmark_service.assert_awaited_once_with()
