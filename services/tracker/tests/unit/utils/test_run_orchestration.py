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
async def test_queued_process_benchmark_bounds_local_pending_contenders(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        concurrency=4,
        priority=3,
        task_ids=["task_0", "task_1", "task_2", "task_3"],
        harness_config=harness_config,
    )
    provider_pool_id = "shared-pool"
    benchmark = _persist_benchmark(
        request,
        database_session,
        queued_pool_id=queue_pool_id(provider_pool_id),
    )

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    get_sandbox_provider = Mock(return_value=sandbox_provider)
    two_started = asyncio.Event()
    hold = asyncio.Event()
    started: list[str] = []

    async def process_task(
        task_row: Task,
        *_args: Any,
        **_kwargs: Any,
    ) -> dict[str, dict[str, Any]]:
        started.append(task_row.task_id)
        if len(started) == 2:
            two_started.set()
        if task_row.task_id == "task_0":
            with Session(bind=database_session.bind) as session:
                task = session.get(Task, task_row.id)
                assert task is not None
                task.status = TaskStatus.IN_PROGRESS
                session.add(task)
                session.commit()

        await hold.wait()

        return {task_row.task_id: {}}

    monkeypatch.setattr("tracker.utils.run_orchestration.process_task", process_task)
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", get_sandbox_provider)

    run = asyncio.create_task(process_benchmark(request.model_dump(), str(benchmark.id), request.task_ids or []))
    try:
        await two_started.wait()

        with Session(bind=database_session.bind) as session:
            statuses = {
                task.task_id: task.status
                for task in session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
            }

        assert started == ["task_0", "task_1"]
        assert statuses == {
            "task_0": TaskStatus.IN_PROGRESS,
            "task_1": TaskStatus.PENDING,
            "task_2": TaskStatus.PENDING,
            "task_3": TaskStatus.PENDING,
        }
    finally:
        run.cancel()
        with suppress(asyncio.CancelledError):
            await run


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_concurrency_decrease_blocks_without_preempting_then_increase_admits(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        concurrency=2,
        priority=3,
        task_ids=["task_0", "task_1"],
        harness_config=harness_config,
    )
    provider_pool_id = "dynamic-pool"
    benchmark = _persist_benchmark(
        request,
        database_session,
        queued_pool_id=queue_pool_id(provider_pool_id),
    )
    first_started = asyncio.Event()
    second_waiting = asyncio.Event()
    check_decrease = asyncio.Event()
    blocked_by_decrease = asyncio.Event()
    capacity_increased = asyncio.Event()
    second_started = asyncio.Event()
    hold = asyncio.Event()

    def set_concurrency(value: int) -> None:
        with Session(bind=database_session.bind) as session:
            persisted_benchmark = session.get(Benchmark, benchmark.id)
            assert persisted_benchmark is not None
            persisted_benchmark.arguments = persisted_benchmark.arguments.model_copy(update={"concurrency": value})
            session.add(persisted_benchmark)
            session.commit()

    async def process_task(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
        if task_row.task_id == "task_0":
            with Session(bind=database_session.bind) as session:
                task = session.get(Task, task_row.id)
                assert task is not None
                task.status = TaskStatus.IN_PROGRESS
                session.add(task)
                session.commit()
            first_started.set()
        else:
            second_waiting.set()
            await check_decrease.wait()
            with Session(bind=database_session.bind) as session:
                persisted_benchmark = session.get(Benchmark, benchmark.id)
                assert persisted_benchmark is not None
                assert persisted_benchmark.arguments.concurrency == 1
            blocked_by_decrease.set()
            await capacity_increased.wait()
            with Session(bind=database_session.bind) as session:
                persisted_benchmark = session.get(Benchmark, benchmark.id)
                assert persisted_benchmark is not None
                assert persisted_benchmark.arguments.concurrency == 2
                task = session.get(Task, task_row.id)
                assert task is not None
                task.status = TaskStatus.IN_PROGRESS
                session.add(task)
                session.commit()
            second_started.set()

        await hold.wait()

        return {task_row.task_id: {}}

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    monkeypatch.setattr("tracker.utils.run_orchestration.process_task", process_task)
    monkeypatch.setattr(
        BenchmarkServiceClient,
        "get_sandbox_provider",
        Mock(return_value=sandbox_provider),
    )

    run = asyncio.create_task(process_benchmark(request.model_dump(), str(benchmark.id), request.task_ids or []))
    try:
        await first_started.wait()
        set_concurrency(1)
        await second_waiting.wait()
        check_decrease.set()
        await blocked_by_decrease.wait()

        with Session(bind=database_session.bind) as session:
            blocked_statuses = {
                task.task_id: task.status
                for task in session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
            }

        assert blocked_statuses == {
            "task_0": TaskStatus.IN_PROGRESS,
            "task_1": TaskStatus.PENDING,
        }

        set_concurrency(2)
        capacity_increased.set()
        await second_started.wait()

        with Session(bind=database_session.bind) as session:
            admitted_statuses = {
                task.task_id: task.status
                for task in session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
            }

        assert admitted_statuses == {
            "task_0": TaskStatus.IN_PROGRESS,
            "task_1": TaskStatus.IN_PROGRESS,
        }
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
    task_ids = ["resume_eval", "pending"]
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
    database_session.add_all(
        [
            Task(
                org_id=TEST_ORG_ID,
                task_id="resume_eval",
                benchmark=benchmark.id,
                status=TaskStatus.EVALUATING,
                eval_resume_state={"cursor": "durable"},
            ),
            Task(
                org_id=TEST_ORG_ID,
                task_id="pending",
                benchmark=benchmark.id,
                status=TaskStatus.PENDING,
            ),
        ]
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
    assert initial_statuses == {
        "resume_eval": TaskStatus.EVALUATING,
        "pending": TaskStatus.PENDING,
    }
    assert benchmark.status == BenchmarkStatus.FINISHED


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
    unrelated_task = Task(
        org_id=TEST_ORG_ID,
        task_id="task_unrelated",
        benchmark=benchmark.id,
        status=TaskStatus.PENDING,
    )
    database_session.add(unrelated_task)
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
        "task_0": TaskStatus.ERROR,
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
        "task_0": TaskStatus.PENDING,
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
