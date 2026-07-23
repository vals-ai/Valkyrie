import asyncio
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
from tracker.scheduler.admission import SandboxQueueContext
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import process_benchmark, reset_to_in_progress_status, start_benchmark_request_to_benchmark


def _persist_benchmark(request: StartBenchmarkRequest, session: Session) -> Benchmark:
    org = Org(id=TEST_ORG_ID, name="default")
    benchmark = start_benchmark_request_to_benchmark(
        request,
        RequestIdentity(org=org, access_key_id=None, email=None, name=None),
    )
    session.add(benchmark)
    session.commit()
    return benchmark


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_process_benchmark_passes_context_and_closes_redis(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tracker.utils.run_orchestration.SANDBOX_QUEUE_ENABLED", True, raising=False)
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        priority=0,
        task_ids=["task_0", "task_1"],
        harness_config=harness_config,
    )
    benchmark = _persist_benchmark(request, database_session)

    queue_redis = AsyncMock()
    expected_context = Mock(spec=SandboxQueueContext)
    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id="shared-pool")
    redis_factory = Mock(return_value=queue_redis)
    context_factory = Mock(return_value=expected_context)
    get_sandbox_provider = Mock(return_value=sandbox_provider)
    all_started = asyncio.Event()
    started: list[str] = []
    task_contexts: list[object] = []

    async def process_task(
        task_row: Task,
        *_args: Any,
        sandbox_provider: SandboxProvider,
        queue_context: SandboxQueueContext | None = None,
        **_kwargs: Any,
    ) -> dict[str, dict[str, Any]]:
        started.append(task_row.task_id)
        task_contexts.append((sandbox_provider, queue_context))
        if len(started) == 2:
            all_started.set()
        await all_started.wait()
        result = {"status": "success", "score": 1.0}
        with Session(bind=database_session.bind) as session:
            task = session.get(Task, task_row.id)
            assert task is not None
            task.status = TaskStatus.FINISHED
            session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, instance_id=task.task_id, result=result))
            session.commit()
        return {task_row.task_id: result}

    monkeypatch.setattr("tracker.utils.run_orchestration.Redis.from_url", redis_factory)
    monkeypatch.setattr("tracker.utils.run_orchestration.create_queue_context", context_factory)
    monkeypatch.setattr("tracker.utils.run_orchestration.process_task", process_task)
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", get_sandbox_provider)

    await asyncio.wait_for(
        process_benchmark(request.model_dump(), str(benchmark.id), request.task_ids or []),
        timeout=2,
    )

    assert started == request.task_ids
    assert task_contexts == [(sandbox_provider, expected_context)] * 2
    get_sandbox_provider.assert_called_once()
    assert context_factory.call_args.kwargs["provider"] is sandbox_provider
    assert context_factory.call_args.kwargs["priority"] == 0
    queue_redis.aclose.assert_awaited_once_with()


@pytest.mark.usefixtures("process_benchmark_env")
async def test_process_benchmark_skips_persisted_queue_priority_when_environment_flag_is_false(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tracker.utils.run_orchestration.SANDBOX_QUEUE_ENABLED", False, raising=False)
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        priority=0,
        task_ids=["task_0"],
        harness_config=harness_config,
    )
    benchmark = _persist_benchmark(request, database_session)

    queue_redis = AsyncMock()
    redis_factory = Mock(return_value=queue_redis)
    context_factory = Mock()
    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id="shared-pool")
    monkeypatch.setattr("tracker.utils.run_orchestration.Redis.from_url", redis_factory)
    monkeypatch.setattr("tracker.utils.run_orchestration.create_queue_context", context_factory)
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", Mock(return_value=sandbox_provider))

    await process_benchmark(request.model_dump(), str(benchmark.id), request.task_ids or [])

    redis_factory.assert_not_called()
    context_factory.assert_not_called()


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
async def test_queued_process_benchmark_reports_provider_configuration_drift(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tracker.utils.run_orchestration.SANDBOX_QUEUE_ENABLED", True, raising=False)
    task_ids = ["task_0", "task_1"]
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        priority=3,
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
    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=None)
    redis_factory = Mock()
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", Mock(return_value=sandbox_provider))
    monkeypatch.setattr("tracker.utils.run_orchestration.Redis.from_url", redis_factory)

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
    redis_factory.assert_not_called()

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
    monkeypatch.setattr("tracker.utils.run_orchestration.SANDBOX_QUEUE_ENABLED", True, raising=False)
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        priority=3,
        task_ids=[],
        harness_config=harness_config,
    )
    benchmark = _persist_benchmark(request, database_session)

    queue_redis = AsyncMock()
    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id="shared-pool")
    failure = RuntimeError("run cleanup failed" if cleanup_fails else "benchmark service close failed")
    close_benchmark_service = AsyncMock(side_effect=None if cleanup_fails else failure)
    monkeypatch.setattr("tracker.utils.run_orchestration.Redis.from_url", Mock(return_value=queue_redis))
    monkeypatch.setattr(
        "tracker.utils.run_orchestration.create_queue_context",
        Mock(return_value=Mock(spec=SandboxQueueContext)),
    )
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", Mock(return_value=sandbox_provider))
    monkeypatch.setattr(BenchmarkServiceClient, "close", close_benchmark_service)
    monkeypatch.setattr(
        "tracker.utils.run_orchestration.catch_errors_during_cleanup",
        Mock(side_effect=failure if cleanup_fails else None),
    )

    with pytest.raises(RuntimeError, match=str(failure)):
        await process_benchmark(request.model_dump(), str(benchmark.id), [])

    close_benchmark_service.assert_awaited_once_with()
    queue_redis.aclose.assert_awaited_once_with()
