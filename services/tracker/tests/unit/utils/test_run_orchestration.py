"""Tests for PostgreSQL-queued run coordination."""

import asyncio
from contextlib import suppress
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from benchmark_service import SandboxProvider
from benchmark_service.client import BenchmarkServiceClient
from sqlmodel import Session, select

from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    EvaluationResult,
    ExecutorDispatch,
    Org,
    Task,
    TaskStatus,
)
from tracker.scheduler.store import queue_pool_id
from tracker.types import HarnessConfig, StartBenchmarkRequest
from tracker.utils import process_benchmark, start_benchmark_request_to_benchmark


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
        aws_managed=False,
        queue_pool_id=queued_pool_id,
    )
    session.add(benchmark)
    session.commit()
    return benchmark


def _queued_run(
    contract: AgentContractRequest,
    harness_config: HarnessConfig,
    session: Session,
    task_ids: list[str],
    *,
    pool_id: str = "coordinator-pool",
    concurrency: int = 5,
) -> tuple[StartBenchmarkRequest, Benchmark]:
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        concurrency=concurrency,
        priority=3,
        task_ids=task_ids,
        harness_config=harness_config,
    )
    return request, _persist_benchmark(request, session, queued_pool_id=queue_pool_id(pool_id))


def _add_tasks(session: Session, benchmark: Benchmark, statuses: dict[str, TaskStatus]) -> None:
    session.add_all(
        [
            Task(
                org_id=TEST_ORG_ID,
                task_id=task_id,
                benchmark=benchmark.id,
                status=status,
                eval_resume_state={"cursor": task_id} if status == TaskStatus.EVALUATING else None,
            )
            for task_id, status in statuses.items()
        ]
    )
    session.commit()


def _finish_task(session: Session, task_row: Task) -> dict[str, Any]:
    result = {"status": "success", "score": 1.0}
    with Session(bind=session.bind) as worker_session:
        task = worker_session.get(Task, task_row.id)
        assert task is not None
        task.status = TaskStatus.FINISHED
        worker_session.add(EvaluationResult(org_id=TEST_ORG_ID, task=task.id, instance_id=None, result=result))
        worker_session.commit()
    return result


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_coordinator_limits_evaluations_and_pending_contenders(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    task_ids = ["evaluation_0", "evaluation_1", "evaluation_2", "pending_0", "pending_1"]
    provider_pool_id = "coordinator-pool"
    request, benchmark = _queued_run(contract, harness_config, database_session, task_ids, concurrency=3)
    _add_tasks(
        database_session,
        benchmark,
        {
            **dict.fromkeys(task_ids[:3], TaskStatus.EVALUATING),
            **dict.fromkeys(task_ids[3:], TaskStatus.PENDING),
            "existing_active": TaskStatus.IN_PROGRESS,
        },
    )
    evaluation_started: list[str] = []
    evaluation_finished: asyncio.Queue[str] = asyncio.Queue()
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
            session.commit()

    async def process_task(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, Any] | None]:
        if task_row.status == TaskStatus.EVALUATING and task_row.eval_resume_state is not None:
            evaluation_started.append(task_row.task_id)
            if len(evaluation_started) == 2:
                two_evaluations_started.set()
            elif len(evaluation_started) == 3:
                third_evaluation_started.set()
            await release_evaluation[task_row.task_id].wait()
            result = _finish_task(database_session, task_row)
            evaluation_finished.put_nowait(task_row.task_id)
            return {task_row.task_id: result}

        assert task_row.status == TaskStatus.PENDING
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
    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
    run = asyncio.create_task(process_benchmark(request.model_dump(), str(benchmark.id), task_ids, **authority_kwargs))
    try:
        await coordinator_polls.get()
        await asyncio.wait_for(two_evaluations_started.wait(), timeout=2)

        assert len(evaluation_started) == 2
        assert pending_started == []

        set_concurrency(2)
        first_evaluation = evaluation_started[0]
        release_evaluation[first_evaluation].set()
        await evaluation_finished.get()
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()

        assert len(evaluation_started) == 2
        assert pending_started == []

        set_concurrency(3)
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()
        await asyncio.wait_for(third_evaluation_started.wait(), timeout=2)

        assert len(evaluation_started) == 3
        assert pending_started == []

        for task_id in evaluation_started[1:]:
            release_evaluation[task_id].set()
            await evaluation_finished.get()
        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()
        await asyncio.wait_for(pending_contender_started.wait(), timeout=2)

        assert len(pending_started) == 1

        continue_coordinator.put_nowait(None)
        await coordinator_polls.get()

        assert len(pending_started) == 1
    finally:
        run.cancel()
        with suppress(asyncio.CancelledError):
            await run


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_process_benchmark_recovers_existing_work_before_finalizing(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    task_ids = ["resume_eval", "recover_build"]
    provider_pool_id = "evaluation-pool"
    request, benchmark = _queued_run(
        contract, harness_config, database_session, task_ids, pool_id=provider_pool_id, concurrency=2
    )
    _add_tasks(
        database_session,
        benchmark,
        {
            "resume_eval": TaskStatus.EVALUATING,
            "recover_build": TaskStatus.BUILDING,
        },
    )
    initial_statuses: dict[str, TaskStatus] = {}
    original_resume_started_at = database_session.exec(
        select(Task.started_at).where(Task.benchmark == benchmark.id).where(Task.task_id == "resume_eval")
    ).one()
    resumed_at: datetime | None = None

    async def recover_queued_pool(_context: object) -> None:
        with Session(bind=database_session.bind) as session:
            task = session.exec(
                select(Task).where(Task.benchmark == benchmark.id).where(Task.task_id == "recover_build")
            ).one()
            assert task.status == TaskStatus.BUILDING
            task.status = TaskStatus.PENDING
            session.commit()

    async def process_task(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
        nonlocal resumed_at
        initial_statuses[task_row.task_id] = task_row.status
        if task_row.task_id == "resume_eval":
            resumed_at = task_row.started_at
        result = _finish_task(database_session, task_row)
        return {task_row.task_id: result}

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    monkeypatch.setattr("tracker.utils.run_orchestration.recover_queued_pool", recover_queued_pool)
    monkeypatch.setattr("tracker.utils.run_orchestration.process_task", process_task)
    monkeypatch.setattr(
        BenchmarkServiceClient,
        "get_sandbox_provider",
        Mock(return_value=sandbox_provider),
    )

    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
    dispatch = database_session.get(ExecutorDispatch, UUID(str(authority_kwargs["executor_dispatch_id"])))
    assert dispatch is not None
    resumed_task = database_session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.task_id == "resume_eval")
    ).one()
    resumed_task.started_at = dispatch.created_at
    database_session.add(resumed_task)
    database_session.commit()
    await asyncio.wait_for(
        process_benchmark(request.model_dump(), str(benchmark.id), task_ids, **authority_kwargs),
        timeout=5,
    )

    database_session.expire_all()
    database_session.refresh(benchmark)
    tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
    assert initial_statuses == {
        "resume_eval": TaskStatus.EVALUATING,
        "recover_build": TaskStatus.PENDING,
    }
    assert {task.task_id: task.status for task in tasks} == {
        "resume_eval": TaskStatus.FINISHED,
        "recover_build": TaskStatus.FINISHED,
    }
    assert resumed_at is not None
    assert resumed_at == dispatch.created_at
    assert resumed_at > original_resume_started_at
    assert benchmark.status == BenchmarkStatus.FINISHED


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_coordinator_retires_stale_evaluation_runner(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    request, benchmark = _queued_run(
        contract,
        harness_config,
        database_session,
        ["resume_eval"],
        pool_id="stale-evaluation-pool",
        concurrency=1,
    )
    _add_tasks(database_session, benchmark, {"resume_eval": TaskStatus.EVALUATING})
    calls = 0

    async def process_task(_task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, None]:
        nonlocal calls
        calls += 1
        return {"resume_eval": None}

    async def recover_queued_pool(_context: object) -> None:
        return None

    provider = Mock(spec=SandboxProvider, admission_pool_id="stale-evaluation-pool")
    monkeypatch.setattr("tracker.utils.run_orchestration.process_task", process_task)
    monkeypatch.setattr("tracker.utils.run_orchestration.recover_queued_pool", recover_queued_pool)
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", Mock(return_value=provider))
    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
    dispatch = database_session.get(ExecutorDispatch, UUID(str(authority_kwargs["executor_dispatch_id"])))
    assert dispatch is not None
    task = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).one()
    task.started_at = dispatch.created_at
    database_session.add(task)
    database_session.commit()

    await asyncio.wait_for(
        process_benchmark(request.model_dump(), str(benchmark.id), ["resume_eval"], **authority_kwargs),
        timeout=2,
    )

    database_session.expire_all()
    persisted_task = database_session.get(Task, task.id)
    assert persisted_task is not None
    assert persisted_task.status == TaskStatus.EVALUATING
    assert calls == 1


@pytest.mark.usefixtures("process_benchmark_env")
async def test_direct_provider_setup_failure_closes_client(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    request = StartBenchmarkRequest(
        benchmark_name="swebench",
        contract=contract,
        task_ids=["task_0"],
        harness_config=harness_config,
    )
    benchmark = _persist_benchmark(request, database_session)
    benchmark_service = AsyncMock(spec=BenchmarkServiceClient)
    benchmark_service.get_sandbox_provider = Mock(side_effect=RuntimeError("provider setup failed"))
    monkeypatch.setattr(
        "tracker.utils.run_orchestration.create_benchmark_service_client_from_request",
        Mock(return_value=benchmark_service),
    )
    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)

    await process_benchmark(request.model_dump(), str(benchmark.id), ["task_0"], **authority_kwargs)

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.ERROR
    assert benchmark.error_message is not None
    assert "provider setup failed" in benchmark.error_message
    benchmark_service.close.assert_awaited_once_with()


@pytest.mark.usefixtures("process_benchmark_env")
async def test_queued_cancellation_errors_owned_work_and_preserves_pending_work(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority_kwargs: Any,
) -> None:
    task_ids = ["active", "pending"]
    request, benchmark = _queued_run(contract, harness_config, database_session, task_ids)
    task_started = asyncio.Event()
    started_task_id: str | None = None

    async def process_task(task_row: Task, *_args: Any, **_kwargs: Any) -> dict[str, None]:
        nonlocal started_task_id
        with Session(bind=database_session.bind) as session:
            task = session.get(Task, task_row.id)
            assert task is not None
            task.status = TaskStatus.IN_PROGRESS
            session.commit()
        started_task_id = task_row.task_id
        task_started.set()
        await asyncio.Event().wait()
        return {task_row.task_id: None}

    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id="coordinator-pool")
    monkeypatch.setattr("tracker.utils.run_orchestration.process_task", process_task)
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", Mock(return_value=sandbox_provider))

    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
    run = asyncio.create_task(process_benchmark(request.model_dump(), str(benchmark.id), task_ids, **authority_kwargs))
    await asyncio.wait_for(task_started.wait(), timeout=2)
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    database_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert benchmark.error_message is None
    task_statuses = {
        task.task_id: task.status
        for task in database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
    }
    assert started_task_id is not None
    assert task_statuses[started_task_id] == TaskStatus.ERROR
    assert list(task_statuses.values()).count(TaskStatus.PENDING) == 1


@pytest.mark.usefixtures("process_benchmark_env")
@pytest.mark.parametrize("provider_pool_id", [None, "different-pool"])
async def test_queued_process_benchmark_reports_provider_configuration_drift(
    contract: AgentContractRequest,
    database_session: Session,
    harness_config: HarnessConfig,
    monkeypatch: pytest.MonkeyPatch,
    provider_pool_id: str | None,
    executor_authority_kwargs: Any,
) -> None:
    task_ids = ["task_0", "task_1"]
    request, benchmark = _queued_run(contract, harness_config, database_session, task_ids, pool_id="expected-pool")
    _add_tasks(
        database_session,
        benchmark,
        {
            "task_0": TaskStatus.BUILDING,
            "task_1": TaskStatus.PENDING,
            "task_unrelated": TaskStatus.PENDING,
        },
    )
    sandbox_provider = Mock(spec=SandboxProvider, admission_pool_id=provider_pool_id)
    monkeypatch.setattr(BenchmarkServiceClient, "get_sandbox_provider", Mock(return_value=sandbox_provider))

    authority_kwargs = executor_authority_kwargs(benchmark, session=database_session)
    await process_benchmark(request.model_dump(), str(benchmark.id), task_ids, **authority_kwargs)

    database_session.refresh(benchmark)
    tasks = database_session.exec(select(Task).where(Task.benchmark == benchmark.id)).all()
    task_statuses = {task.task_id: task.status for task in tasks}
    assert benchmark.status == BenchmarkStatus.ERROR
    expected_error = (
        "Sandbox provider configuration is unavailable"
        if provider_pool_id is None
        else "Configured sandbox provider does not match the run's queued provider pool"
    )
    assert benchmark.error_message is not None and expected_error in benchmark.error_message
    assert task_statuses == {
        "task_unrelated": TaskStatus.PENDING,
        "task_0": TaskStatus.ERROR,
        "task_1": TaskStatus.ERROR,
    }
