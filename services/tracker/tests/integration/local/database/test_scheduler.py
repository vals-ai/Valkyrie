"""Run with `uv run pytest tests/integration/local/database/test_scheduler.py`.

Exercise PostgreSQL-backed sandbox scheduling against disposable PostgreSQL.
"""

import asyncio
from asyncio import Semaphore
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

from benchmark_service import (
    ImageSource,
    Resources,
    Sandbox,
    SandboxProvider,
    SandboxProviderConfig,
    SandboxSource,
)
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.schemas import RetrieveTaskResponse
from fastapi import HTTPException, Request
import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine, func, select
from tenacity import wait_none

from tests.factories import make_task
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    Org,
    RetryMode,
    Task,
    TaskStatus,
)
from tracker.exceptions import SandboxSetupError
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.executor.release_control import promote_release
import tracker.scheduler.admission as admission
import tracker.scheduler.store as store
from tracker.types import HarnessConfig
from tracker.utils import task_execution
import main as tracker_main

_ATTEMPT = datetime(2026, 7, 27, 12)
_SOURCE = ImageSource(image="scheduler-test-image")
_RESOURCES = Resources(vcpu=1, memory=2, disk=3)


class MockProvider:
    def __init__(
        self,
        pool_id: str,
        events: list[str],
        *,
        capacity: Sequence[bool] = (True,),
        on_check: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.admission_pool_id = pool_id
        self.events = events
        self.capacity = iter(capacity)
        self.on_check = on_check

    async def check_admission(self, _source: SandboxSource, _resources: Resources) -> bool:
        self.events.append("capacity")
        if self.on_check:
            await self.on_check()
        return next(self.capacity, True)


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


def _update_task(engine: Engine, task: Task, status: TaskStatus, started_at: datetime) -> None:
    with Session(engine) as session:
        row = session.get(Task, task.id)
        assert row
        row.status = status
        row.started_at = started_at
        session.commit()


def _task(engine: Engine, task: Task) -> Task:
    with Session(engine) as session:
        row = session.get(Task, task.id)
        assert row
        return row


def _set_concurrency(engine: Engine, benchmark: Benchmark, concurrency: int) -> None:
    with Session(engine) as session:
        row = session.get(Benchmark, benchmark.id)
        assert row
        row.arguments = row.arguments.model_copy(update={"concurrency": concurrency})
        session.commit()


def _revoke_dispatch(engine: Engine, authority: ExecutionAuthority) -> None:
    with Session(engine) as session:
        dispatch = session.get(ExecutorDispatch, authority.dispatch_id)
        assert dispatch is not None
        dispatch.status = ExecutorDispatchStatus.FAILED
        session.commit()


def _sandbox(
    events: list[str],
    *,
    name: str = "sandbox",
    on_create: Callable[[], Awaitable[None]] | None = None,
    on_cleanup: Callable[[], Awaitable[None]] | None = None,
) -> Callable[[], AbstractAsyncContextManager[Sandbox]]:
    @asynccontextmanager
    async def context() -> AsyncGenerator[Sandbox]:
        events.append("create")
        if on_create:
            await on_create()
        sandbox = Mock(id=f"sandbox-{len(events)}", name=name)
        try:
            yield cast(Sandbox, sandbox)
        finally:
            if on_cleanup:
                await on_cleanup()
            events.append("cleanup")

    return context


def _context(
    engine: Engine,
    provider_pool_id: str,
    events: list[str],
    *,
    capacity: Sequence[bool] = (True,),
    on_check: Callable[[], Awaitable[None]] | None = None,
) -> admission.SandboxQueueContext:
    provider = MockProvider(provider_pool_id, events, capacity=capacity, on_check=on_check)
    return admission.SandboxQueueContext(
        provider=cast(SandboxProvider, provider),
        pool_id=store.queue_pool_id(provider_pool_id),
        engine=engine,
        poll_interval_seconds=0,
    )


def test_queue_context_requires_managed_provider() -> None:
    provider = Mock(admission_pool_id=None)

    with pytest.raises(ValueError, match="does not support queued admission"):
        admission.create_queue_context(
            engine=cast(Engine, Mock()),
            provider=cast(SandboxProvider, provider),
        )


async def _enter(
    stack: AsyncExitStack,
    context: admission.SandboxQueueContext,
    task: Task,
    events: list[str],
    authority: Any,
    *,
    on_create: Callable[[], Awaitable[None]] | None = None,
    on_cleanup: Callable[[], Awaitable[None]] | None = None,
) -> Sandbox | None:
    return await admission.enter_queued_sandbox(
        stack=stack,
        context=context,
        task_row_id=task.id,
        expected_started_at=task.started_at,
        authority=authority,
        source=_SOURCE,
        resources=_RESOURCES,
        create=_sandbox(events, on_create=on_create, on_cleanup=on_cleanup),
    )


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


async def test_admission_reuses_the_advisory_lock_connection(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session,
        store.queue_pool_id(provider_pool_id),
        [("single-connection", TaskStatus.PENDING, _ATTEMPT)],
    )
    single_connection_engine = create_engine(
        postgres_engine.url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.05,
    )
    events: list[str] = []
    authority = executor_authority(benchmark, session=postgres_session)
    try:
        async with AsyncExitStack() as stack:
            assert await _enter(
                stack,
                _context(single_connection_engine, provider_pool_id, events),
                task,
                events,
                authority,
            )
    finally:
        single_connection_engine.dispose()

    assert _task(postgres_engine, task).status == TaskStatus.IN_PROGRESS
    assert events == ["capacity", "create", "cleanup"]


async def test_revocation_before_claim_creates_no_sandbox(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session,
        store.queue_pool_id(provider_pool_id),
        [("revoked-before-claim", TaskStatus.PENDING, _ATTEMPT)],
    )
    authority = executor_authority(benchmark, session=postgres_session)
    events: list[str] = []

    async def revoke() -> None:
        _revoke_dispatch(postgres_engine, authority)

    async with AsyncExitStack() as stack:
        sandbox = await _enter(
            stack,
            _context(postgres_engine, provider_pool_id, events, on_check=revoke),
            task,
            events,
            authority,
        )

    assert sandbox is None
    assert _task(postgres_engine, task).status == TaskStatus.PENDING
    assert events == ["capacity"]


async def test_revocation_during_creation_cleans_up_without_starting(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    _, benchmark, (task,) = _run(
        postgres_session,
        store.queue_pool_id(provider_pool_id),
        [("revoked-during-creation", TaskStatus.PENDING, _ATTEMPT)],
    )
    authority = executor_authority(benchmark, session=postgres_session)
    events: list[str] = []

    async def revoke() -> None:
        _revoke_dispatch(postgres_engine, authority)

    async with AsyncExitStack() as stack:
        sandbox = await _enter(
            stack,
            _context(postgres_engine, provider_pool_id, events),
            task,
            events,
            authority,
            on_create=revoke,
        )

    assert sandbox is None
    assert _task(postgres_engine, task).status == TaskStatus.BUILDING
    assert events == ["capacity", "create", "cleanup"]


async def test_priority_fifo_pool_isolation_and_build_recovery(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    other_pool_id = store.queue_pool_id(f"daytona:{uuid4()}")
    _, _, (low_priority,) = _run(
        postgres_session,
        pool_id,
        [("low", TaskStatus.PENDING, _ATTEMPT - timedelta(minutes=1))],
        priority=4,
    )
    _, _, (head,) = _run(
        postgres_session,
        pool_id,
        [("head", TaskStatus.PENDING, _ATTEMPT)],
        priority=0,
    )
    _, _, (next_fifo,) = _run(
        postgres_session,
        pool_id,
        [("next", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1))],
        priority=0,
    )
    _, _, (other_pool,) = _run(
        postgres_session,
        other_pool_id,
        [("other", TaskStatus.PENDING, _ATTEMPT)],
    )

    assert not store.eligible_task_is(postgres_session, pool_id, low_priority.id, low_priority.started_at)
    assert store.eligible_task_is(postgres_session, pool_id, head.id, head.started_at)
    assert not store.claim_eligible_task(postgres_session, pool_id, next_fifo.id, next_fifo.started_at)
    assert store.eligible_task_is(postgres_session, other_pool_id, other_pool.id, other_pool.started_at)
    assert not store.claim_eligible_task(
        postgres_session,
        pool_id,
        head.id,
        head.started_at + timedelta(microseconds=1),
    )
    assert store.claim_eligible_task(postgres_session, pool_id, head.id, head.started_at)
    postgres_session.commit()

    await admission.recover_queued_pool(_context(postgres_engine, provider_pool_id, []))

    recovered = _task(postgres_engine, head)
    assert recovered.status == TaskStatus.PENDING
    assert recovered.started_at > head.started_at


async def test_admission_rechecks_dynamic_concurrency_capacity_and_lock(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    _, benchmark, (active, waiting) = _run(
        postgres_session,
        pool_id,
        [
            ("active", TaskStatus.IN_PROGRESS, _ATTEMPT),
            ("waiting", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
    )
    events: list[str] = []
    lock_checks: list[bool] = []
    observed_states: list[TaskStatus] = []

    authority = executor_authority(benchmark, session=postgres_session)

    async def observe_lock() -> None:
        async with store.queue_pool_lock(postgres_engine, pool_id) as acquired:
            lock_checks.append(acquired)
        observed_states.append(_task(postgres_engine, waiting).status)

    async def poll(_seconds: float) -> None:
        _set_concurrency(postgres_engine, benchmark, 2)

    sleep = AsyncMock(side_effect=poll)
    monkeypatch.setattr("tracker.scheduler.admission.asyncio.sleep", sleep)
    context = _context(
        postgres_engine,
        provider_pool_id,
        events,
        capacity=(False, True),
        on_check=observe_lock,
    )
    async with AsyncExitStack() as stack:
        sandbox = await _enter(stack, context, waiting, events, authority, on_create=observe_lock)
        assert sandbox
        assert _task(postgres_engine, waiting).status == TaskStatus.IN_PROGRESS

    assert _task(postgres_engine, active).status == TaskStatus.IN_PROGRESS
    assert sleep.await_count == 2
    assert lock_checks == [False, False, False]
    assert observed_states == [TaskStatus.PENDING, TaskStatus.PENDING, TaskStatus.BUILDING]
    assert events == ["capacity", "capacity", "create", "cleanup"]


async def test_cancellation_releases_pool_lock(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    _, benchmark, (task,) = _run(
        postgres_session,
        pool_id,
        [("cancelled", TaskStatus.PENDING, _ATTEMPT)],
    )
    authority = executor_authority(benchmark, session=postgres_session)
    capacity_started = asyncio.Event()
    hold_capacity = asyncio.Event()

    async def block_capacity() -> None:
        capacity_started.set()
        await hold_capacity.wait()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter(
                stack,
                _context(postgres_engine, provider_pool_id, [], on_check=block_capacity),
                task,
                [],
                authority,
            )
        )
        await capacity_started.wait()
        entering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await entering

    async with store.queue_pool_lock(postgres_engine, pool_id) as acquired:
        pass
    assert acquired is True


async def test_stale_creation_is_cleaned_before_cancellation(
    postgres_engine: Engine,
    postgres_session: Session,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    _, benchmark, (capacity_task, creation_task) = _run(
        postgres_session,
        pool_id,
        [
            ("stale-capacity", TaskStatus.PENDING, _ATTEMPT),
            ("stale-creation", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
    authority = executor_authority(benchmark, session=postgres_session)
    capacity_events: list[str] = []

    async def supersede_during_capacity() -> None:
        _update_task(
            postgres_engine,
            capacity_task,
            TaskStatus.STOPPED,
            capacity_task.started_at + timedelta(microseconds=1),
        )

    async with AsyncExitStack() as stack:
        assert (
            await _enter(
                stack,
                _context(postgres_engine, provider_pool_id, capacity_events, on_check=supersede_during_capacity),
                capacity_task,
                capacity_events,
                authority,
            )
            is None
        )

    events: list[str] = []
    cleanup_started = asyncio.Event()
    finish_cleanup = asyncio.Event()

    async def supersede_attempt() -> None:
        _update_task(
            postgres_engine,
            creation_task,
            TaskStatus.PENDING,
            creation_task.started_at + timedelta(microseconds=1),
        )

    async def block_cleanup() -> None:
        cleanup_started.set()
        await finish_cleanup.wait()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter(
                stack,
                _context(postgres_engine, provider_pool_id, events),
                creation_task,
                events,
                authority,
                on_create=supersede_attempt,
                on_cleanup=block_cleanup,
            )
        )
        await cleanup_started.wait()
        entering.cancel()
        await asyncio.sleep(0)
        assert not entering.done()
        finish_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await entering

    assert capacity_events == ["capacity"]
    assert _task(postgres_engine, creation_task).status == TaskStatus.PENDING
    assert events == ["capacity", "create", "cleanup"]


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
                harness_config=harness_config,
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


async def test_two_recovery_handoffs_leave_one_evaluation_owner(
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
        [("raced-evaluation", TaskStatus.EVALUATING, _ATTEMPT)],
    )
    task.eval_resume_state = {"job_id": "raced-job"}
    postgres_session.add(task)
    postgres_session.commit()
    executor_authority(benchmark, session=postgres_session)
    assert benchmark.current_execution_release_id is not None
    promote_release(postgres_session, benchmark.current_execution_release_id)
    postgres_session.commit()
    first_enqueued = asyncio.Event()
    release_first = asyncio.Event()
    enqueued_dispatch_ids: list[UUID] = []

    async def enqueue(dispatch: ExecutorDispatch, *, session: Session, **_kwargs: object) -> None:
        persisted_dispatch = session.get(ExecutorDispatch, dispatch.id)
        assert persisted_dispatch is not None
        persisted_dispatch.status = ExecutorDispatchStatus.RUNNING
        session.commit()
        enqueued_dispatch_ids.append(dispatch.id)
        if len(enqueued_dispatch_ids) == 1:
            first_enqueued.set()
            await release_first.wait()

    monkeypatch.setattr(tracker_main, "_enqueue_executor_dispatch", enqueue)

    async def recover() -> None:
        with Session(postgres_engine) as recovery_session:
            response = await tracker_main.retry_or_resume_benchmark(
                benchmark.id,
                Request({"type": "http", "headers": []}),
                retry=False,
                retry_mode=RetryMode.AUTO,
                concurrency=None,
                task_ids=[],
                service_headers={},
                secrets={},
                benchmark_url=None,
                session=recovery_session,
                harness_config=harness_config,
                org=org,
            )
            assert response.status == "success"

    first_recovery = asyncio.create_task(recover())
    await asyncio.wait_for(first_enqueued.wait(), timeout=5)
    await asyncio.wait_for(recover(), timeout=5)
    release_first.set()
    await asyncio.wait_for(first_recovery, timeout=5)

    assert len(enqueued_dispatch_ids) == 2
    first_dispatch = postgres_session.get(ExecutorDispatch, enqueued_dispatch_ids[0])
    winning_dispatch = postgres_session.get(ExecutorDispatch, enqueued_dispatch_ids[1])
    assert first_dispatch is not None
    assert winning_dispatch is not None
    postgres_session.expire_all()
    winning_task = postgres_session.get(Task, task.id)
    assert winning_task is not None
    assert winning_task.started_at == winning_dispatch.created_at

    with Session(postgres_engine) as stale_session:
        stale_task = stale_session.get(Task, task.id)
        assert stale_task is not None
        stale_session.expunge(stale_task)
    stale_task.started_at = first_dispatch.created_at
    service = AsyncMock(spec=BenchmarkServiceClient)
    service.resume_evaluation.return_value = {"score": 1.0}
    monkeypatch.setattr(task_execution, "engine", postgres_engine)
    monkeypatch.setattr(task_execution, "buffer_logs", Mock())
    request = benchmark.start_benchmark_request(harness_config)

    async def run(task_row: Task, authority: ExecutionAuthority) -> dict[str, dict[str, Any] | None]:
        return await task_execution.process_task(
            task_row,
            request,
            cast(BenchmarkServiceClient, service),
            benchmark.id,
            task.task_id,
            harness_config,
            org,
            sandbox_provider_config=cast(SandboxProviderConfig, object()),
            sandbox_provider=cast(SandboxProvider, object()),
            creation_semaphore=Semaphore(1),
            authority=authority,
        )

    stale_result = await run(stale_task, ExecutionAuthority(benchmark.id, first_dispatch.id))
    winner_result = await run(winning_task, ExecutionAuthority(benchmark.id, winning_dispatch.id))

    assert stale_result == {task.task_id: None}
    assert winner_result == {task.task_id: {"score": 1.0}}
    service.resume_evaluation.assert_awaited_once()
    assert _task(postgres_engine, task).status == TaskStatus.FINISHED


async def test_resumed_evaluation_uses_lock_connection_for_callback_and_finalization(
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
        [("resume-on-one-connection", TaskStatus.EVALUATING, _ATTEMPT)],
    )
    task.eval_resume_state = {"job_id": "initial-job"}
    postgres_session.add(task)
    postgres_session.commit()
    authority = executor_authority(benchmark, session=postgres_session)
    dispatch = postgres_session.get(ExecutorDispatch, authority.dispatch_id)
    assert dispatch is not None
    task.started_at = dispatch.created_at
    postgres_session.add(task)
    postgres_session.commit()

    service = AsyncMock(spec=BenchmarkServiceClient)

    async def resume_evaluation(*_args: object, on_eval_resume_state: Any, **_kwargs: object) -> dict[str, float]:
        on_eval_resume_state({"job_id": "updated-job"})
        return {"score": 1.0}

    service.resume_evaluation.side_effect = resume_evaluation
    single_connection_engine = create_engine(
        postgres_engine.url,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.05,
    )
    monkeypatch.setattr(task_execution, "engine", single_connection_engine)
    monkeypatch.setattr(task_execution, "buffer_logs", Mock())
    request = benchmark.start_benchmark_request(harness_config)
    try:
        result = await task_execution.process_task(
            task,
            request,
            cast(BenchmarkServiceClient, service),
            benchmark.id,
            task.task_id,
            harness_config,
            org,
            sandbox_provider_config=cast(SandboxProviderConfig, object()),
            sandbox_provider=cast(SandboxProvider, object()),
            creation_semaphore=Semaphore(1),
            authority=authority,
        )
    finally:
        single_connection_engine.dispose()

    assert result == {task.task_id: {"score": 1.0}}
    service.resume_evaluation.assert_awaited_once()
    persisted_task = _task(postgres_engine, task)
    assert persisted_task.status == TaskStatus.FINISHED
    assert persisted_task.eval_resume_state == {"job_id": "updated-job"}


async def test_setup_retry_reenters_fifo_before_competitor(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
    executor_authority: Any,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    org, benchmark, (retrying, competitor) = _run(
        postgres_session,
        pool_id,
        [
            ("retrying", TaskStatus.PENDING, _ATTEMPT),
            ("competitor", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
    )
    events: list[str] = []
    admission_states: list[tuple[TaskStatus, TaskStatus]] = []

    async def observe_admission() -> None:
        admission_states.append(
            (
                _task(postgres_engine, retrying).status,
                _task(postgres_engine, competitor).status,
            )
        )

    names: list[str] = []

    def create_sandbox(*_args: object, **kwargs: object) -> AbstractAsyncContextManager[Sandbox]:
        names.append(cast(str, kwargs["sandbox_name"]))
        return _sandbox(events, name=names[-1])()

    service = AsyncMock(spec=BenchmarkServiceClient)
    service.retrieve_task.return_value = RetrieveTaskResponse(
        source=_SOURCE,
        problem_path="/tmp/problem.txt",
        cwd="/testbed",
        resources=_RESOURCES,
    )
    service.evaluate_instance.return_value = {"status": "success", "score": 1.0}
    retryable_attempt = getattr(task_execution, "_process_task_attempt")
    monkeypatch.setattr(retryable_attempt.retry, "wait", wait_none())
    monkeypatch.setattr(task_execution, "engine", postgres_engine)
    monkeypatch.setattr(task_execution, "buffer_logs", Mock())
    monkeypatch.setattr(task_execution, "create_sandbox", create_sandbox)
    monkeypatch.setattr(
        task_execution,
        "upload_agent_artifacts",
        AsyncMock(side_effect=[SandboxSetupError("retry"), None]),
    )
    monkeypatch.setattr(task_execution, "run_agent", AsyncMock(return_value=(None, 0.0)))
    context = _context(postgres_engine, provider_pool_id, events, on_check=observe_admission)
    authority = executor_authority(benchmark, session=postgres_session)

    await task_execution.process_task(
        retrying,
        benchmark.start_benchmark_request(harness_config),
        cast(BenchmarkServiceClient, service),
        benchmark.id,
        retrying.task_id,
        harness_config,
        org,
        sandbox_provider_config=cast(SandboxProviderConfig, object()),
        sandbox_provider=context.provider,
        creation_semaphore=Semaphore(1),
        authority=authority,
        queue_context=context,
    )

    assert admission_states == [
        (TaskStatus.PENDING, TaskStatus.PENDING),
        (TaskStatus.PENDING, TaskStatus.PENDING),
    ]
    assert _task(postgres_engine, competitor).status == TaskStatus.PENDING
    assert len(names) == 2
    assert names[0] == names[1]
    assert retrying.id.hex in names[0]
