"""Run with `uv run pytest tests/integration/local/database/test_scheduler.py`.

Exercise PostgreSQL-backed sandbox scheduling against disposable PostgreSQL.
"""

import asyncio
import threading
from collections.abc import AsyncGenerator, Generator, Sequence
from datetime import datetime
from hashlib import sha256
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import httpx
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import VerifyTaskIdsResponse
from fastapi import HTTPException, Request
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session, func, select

from tests.factories import make_task
from tracker.auth import RequestIdentity
from tracker.aws.resolver import AWSRuntimeResolution
from tracker.aws.runtime import AWSRuntime
from tracker.aws.services import CloudRuntimeFactory
from tracker.runtime.services import RuntimeServices
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    BenchmarkArguments,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorRelease,
    Org,
    RetryMode,
    Task,
    TaskStatus,
)
import tracker.executor.dispatch_control as dispatch_control
from tracker.executor.release_control import promote_release
import tracker.scheduler.store as store
from tracker.types import HarnessConfig, StartBenchmarkRequest
import main as tracker_main

_ATTEMPT = datetime(2026, 7, 27, 12)


def _use_access_key_runtime(monkeypatch: pytest.MonkeyPatch, harness_config: HarnessConfig) -> None:
    monkeypatch.setattr(
        tracker_main,
        "resolve_run_aws_runtime_and_access_key_config",
        Mock(return_value=AWSRuntimeResolution(AWSRuntime.from_harness_config(harness_config), harness_config)),
    )


def _run(
    session: Session,
    pool_id: str,
    tasks: Sequence[tuple[str, TaskStatus, datetime]],
    *,
    priority: int = 3,
    concurrency: int = 1,
) -> tuple[Org, Benchmark, list[Task]]:
    org = Org(id=uuid4(), name=f"scheduler-{uuid4()}")
    benchmark = Benchmark(
        org_id=org.id,
        name=f"run-{uuid4()}",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
            concurrency=concurrency,
            priority=priority,
            queue_pool_id=pool_id,
        ),
    )
    rows = [
        make_task(benchmark, task_id, status=status, started_at=started_at) for task_id, status, started_at in tasks
    ]
    session.add(org)
    session.flush()
    session.add(benchmark)
    session.add_all(rows)
    session.commit()
    return org, benchmark, rows


@pytest.mark.parametrize("operation", ["retry", "start"])
async def test_http_admission_waits_for_real_postgres_row_lock_without_blocking_loop(
    operation: str,
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
    executor_authority: Any,
) -> None:
    org, benchmark, _ = _run(
        postgres_session,
        store.queue_pool_id(f"daytona:{uuid4()}"),
        [("locked-admission", TaskStatus.STOPPED, _ATTEMPT)],
    )
    executor_authority(benchmark, session=postgres_session)
    assert benchmark.current_execution_release_id is not None
    benchmark.status = BenchmarkStatus.STOPPED
    postgres_session.add(benchmark)
    promote_release(postgres_session, benchmark.current_execution_release_id)
    postgres_session.commit()
    identity = RequestIdentity(org=org, access_key_id=None, email=None, name=None)

    def request_session() -> Generator[Session, None, None]:
        with Session(postgres_engine) as session:
            yield session

    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_session, request_session)
    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_current_org, lambda: org)
    monkeypatch.setitem(tracker_main.app.dependency_overrides, tracker_main.get_current_starter, lambda: identity)
    monkeypatch.setattr(tracker_main, "check_database_connection", lambda: True)
    monkeypatch.setattr(tracker_main, "SANDBOX_QUEUE_ENABLED", False)
    _use_access_key_runtime(monkeypatch, harness_config)

    async def health_check(*_args: Any, **_kwargs: Any) -> object:
        return object()

    async def verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        return VerifyTaskIdsResponse(task_ids=["new-task"])

    async def close(*_args: Any, **_kwargs: Any) -> None:
        return None

    async def copy_agent(*_args: Any, **_kwargs: Any) -> None:
        return None

    enqueued: list[UUID] = []

    async def enqueue(dispatch: ExecutorDispatch, **_kwargs: Any) -> None:
        enqueued.append(dispatch.id)

    monkeypatch.setattr(BenchmarkServiceClient, "health_check", health_check)
    monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", verify_task_ids)
    monkeypatch.setattr(BenchmarkServiceClient, "close", close)
    monkeypatch.setattr(tracker_main, "copy_agent_to_benchmark", copy_agent)
    monkeypatch.setattr(tracker_main, "_enqueue_executor_dispatch", enqueue)
    lock_entered = threading.Event()
    if operation == "retry":
        original_lock = tracker_main.lock_executor_admission

        def observed_recovery_lock(session: Session) -> object:
            session.exec(text("SET LOCAL lock_timeout = '5s'"))
            lock_entered.set()
            return original_lock(session)

        monkeypatch.setattr(tracker_main, "lock_executor_admission", observed_recovery_lock)
        url = f"/retry-or-resume-benchmark/{benchmark.id}"
        body: dict[str, Any] = {}
    else:
        original_select_active_release = dispatch_control.select_active_release

        def observed_start_lock(session: Session, *, for_update: bool = False) -> ExecutorRelease:
            if for_update:
                session.exec(text("SET LOCAL lock_timeout = '5s'"))
                lock_entered.set()
            return original_select_active_release(session, for_update=for_update)

        monkeypatch.setattr(dispatch_control, "select_active_release", observed_start_lock)
        url = "/start-benchmark"
        body = StartBenchmarkRequest(
            benchmark_name=f"postgres-lock-{uuid4()}",
            contract=AgentContractRequest(name="agent", install_cmd="true", run_cmd="true"),
            task_ids=["new-task"],
            harness_config=harness_config,
        ).model_dump(mode="json")

    request: asyncio.Task[httpx.Response] | None = None
    with Session(postgres_engine) as blocker:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=tracker_main.app), base_url="http://test"
        ) as client:
            try:
                blocker.exec(select(ExecutorAdmission).with_for_update()).one()
                request = asyncio.create_task(client.post(url, json=body))
                assert await asyncio.to_thread(lock_entered.wait, 5)
                assert not request.done()
                health = await asyncio.wait_for(client.get("/health"), timeout=2)
                assert health.status_code == 200
                blocker.rollback()
                response = await asyncio.wait_for(request, timeout=5)
            finally:
                blocker.rollback()
                if request is not None:
                    await asyncio.wait_for(
                        asyncio.gather(request, return_exceptions=True),
                        timeout=5,
                    )

    assert response.status_code == 200, response.text
    assert len(enqueued) == 1


async def test_pool_locks_are_isolated_and_reusable(postgres_engine: Engine) -> None:
    first_pool = store.queue_pool_id(f"daytona:{uuid4()}")
    second_pool = store.queue_pool_id(f"daytona:{uuid4()}")

    async with store.queue_pool_lock(postgres_engine, first_pool) as first:
        async with store.queue_pool_lock(postgres_engine, first_pool) as duplicate:
            pass
        async with store.queue_pool_lock(postgres_engine, second_pool) as independent:
            pass
    async with store.queue_pool_lock(postgres_engine, first_pool) as reused:
        pass

    assert (first, duplicate, independent, reused) == (True, False, True, True)


async def test_queue_pool_lock_contends_with_legacy_key(postgres_engine: Engine) -> None:
    pool_id = store.queue_pool_id(f"daytona:{uuid4()}")
    legacy_lock_key = int.from_bytes(sha256(pool_id.encode()).digest()[:8], byteorder="big", signed=True)

    with postgres_engine.connect() as legacy_connection:
        legacy_acquired = bool(
            legacy_connection.execute(
                text("SELECT pg_try_advisory_lock(:lock_key)"),
                {"lock_key": legacy_lock_key},
            ).scalar_one()
        )
        legacy_connection.commit()
        assert legacy_acquired
        try:
            async with store.queue_pool_lock(postgres_engine, pool_id) as new_version_acquired:
                pass
            assert new_version_acquired is False
        finally:
            legacy_released = bool(
                legacy_connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_key)"),
                    {"lock_key": legacy_lock_key},
                ).scalar_one()
            )
            legacy_connection.commit()
            assert legacy_released


async def test_cancelled_evaluation_releases_and_reuses_task_lock(postgres_engine: Engine) -> None:
    task_row_id = uuid4()
    acquired = asyncio.Event()
    hold = asyncio.Event()

    async def owner() -> None:
        async with store.task_evaluation_lock(postgres_engine, task_row_id) as owns_lock:
            assert owns_lock
            acquired.set()
            await hold.wait()

    owner_task = asyncio.create_task(owner())
    await acquired.wait()
    owner_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner_task

    async with store.task_evaluation_lock(postgres_engine, task_row_id) as reused:
        pass
    assert reused is True


async def test_held_evaluation_lock_rejects_recovery_without_mutation(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    org, benchmark, (task,) = _run(
        postgres_session,
        store.queue_pool_id(provider_pool_id),
        [("owned-evaluation", TaskStatus.EVALUATING, _ATTEMPT)],
    )
    task.eval_resume_state = {"job_id": "active-job"}
    postgres_session.add(task)
    postgres_session.commit()
    authority = executor_authority(benchmark, session=postgres_session)
    dispatch = postgres_session.get(ExecutorDispatch, authority.dispatch_id)
    assert dispatch is not None
    task.started_at = dispatch.created_at
    postgres_session.add(task)
    postgres_session.commit()
    original_started_at = task.started_at
    dispatch_count = postgres_session.exec(
        select(func.count()).select_from(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark.id)
    ).one()
    enqueue = AsyncMock()
    monkeypatch.setattr(tracker_main, "_enqueue_executor_dispatch", enqueue)
    _use_access_key_runtime(monkeypatch, harness_config)

    async with store.task_evaluation_lock(postgres_engine, task.id) as acquired:
        assert acquired
        with pytest.raises(HTTPException) as error:
            await tracker_main.retry_or_resume_benchmark(
                benchmark.id,
                Request({"type": "http", "headers": []}),
                retry=False,
                retry_mode=RetryMode.AUTO,
                concurrency=None,
                task_ids=[],
                service_headers={},
                secrets={},
                benchmark_url=None,
                session=postgres_session,
                org=org,
            )

    assert error.value.status_code == 409
    assert error.value.detail == "Run has an evaluation that is already owned by an active executor"
    enqueue.assert_not_awaited()
    postgres_session.expire_all()
    persisted_task = postgres_session.get(Task, task.id)
    assert persisted_task is not None
    assert persisted_task.status == TaskStatus.EVALUATING
    assert persisted_task.started_at == original_started_at
    assert (
        postgres_session.exec(
            select(func.count()).select_from(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark.id)
        ).one()
        == dispatch_count
    )


@pytest.fixture
async def runtime_services(harness_config: HarnessConfig) -> AsyncGenerator[RuntimeServices, None]:
    """Open real AWS adapters while tests replace their external calls."""
    aws_runtime = AWSRuntime.from_harness_config(harness_config)
    runtime = CloudRuntimeFactory.create_runtime(
        aws_runtime,
    )
    yield runtime
