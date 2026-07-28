"""PostgreSQL scheduler behavior tests."""

import asyncio
from asyncio import Semaphore
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta
from typing import cast
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

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
import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine
from tenacity import wait_none

from tests.factories import make_task
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    Org,
    Task,
    TaskStatus,
)
from tracker.exceptions import SandboxSetupError
import tracker.scheduler.admission as admission
import tracker.scheduler.store as store
from tracker.types import HarnessConfig
from tracker.utils import task_execution

_ATTEMPT = datetime(2026, 7, 27, 12)
_SOURCE = ImageSource(image="scheduler-test-image")
_RESOURCES = Resources(vcpu=1, memory=2, disk=3)


class _Provider:
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
    provider = _Provider(provider_pool_id, events, capacity=capacity, on_check=on_check)
    return admission.SandboxQueueContext(
        provider=cast(SandboxProvider, provider),
        pool_id=store.queue_pool_id(provider_pool_id),
        engine=engine,
        poll_interval_seconds=0,
    )


async def _enter(
    stack: AsyncExitStack,
    context: admission.SandboxQueueContext,
    task: Task,
    events: list[str],
    *,
    on_create: Callable[[], Awaitable[None]] | None = None,
    on_cleanup: Callable[[], Awaitable[None]] | None = None,
) -> Sandbox | None:
    return await admission.enter_queued_sandbox(
        stack=stack,
        context=context,
        task_row_id=task.id,
        expected_started_at=task.started_at,
        source=_SOURCE,
        resources=_RESOURCES,
        create=_sandbox(events, on_create=on_create, on_cleanup=on_cleanup),
    )


async def test_pool_locks_are_isolated_and_reusable(postgres_engine: Engine) -> None:
    first_pool = store.queue_pool_id(f"daytona:{uuid4()}")
    second_pool = store.queue_pool_id(f"daytona:{uuid4()}")

    async with store.PostgresPoolLock(postgres_engine, first_pool) as first:
        async with store.PostgresPoolLock(postgres_engine, first_pool) as duplicate:
            pass
        async with store.PostgresPoolLock(postgres_engine, second_pool) as independent:
            pass
    async with store.PostgresPoolLock(postgres_engine, first_pool) as reused:
        pass

    assert (first, duplicate, independent, reused) == (True, False, True, True)


async def test_admission_reuses_the_advisory_lock_connection(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    _, _, (task,) = _run(
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
    try:
        async with AsyncExitStack() as stack:
            assert await _enter(stack, _context(single_connection_engine, provider_pool_id, events), task, events)
    finally:
        single_connection_engine.dispose()

    assert _task(postgres_engine, task).status == TaskStatus.IN_PROGRESS
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

    await admission.recover_queued_pool(_context(postgres_engine, provider_pool_id, []))

    recovered = _task(postgres_engine, head)
    assert recovered.status == TaskStatus.PENDING
    assert recovered.started_at > head.started_at


async def test_admission_rechecks_dynamic_concurrency_capacity_and_lock(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
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

    async def observe_lock() -> None:
        async with store.PostgresPoolLock(postgres_engine, pool_id) as acquired:
            lock_checks.append(acquired)
        observed_states.append(_task(postgres_engine, waiting).status)

    async def poll(_seconds: float) -> None:
        _set_concurrency(postgres_engine, benchmark, 2)

    sleep = AsyncMock(side_effect=poll)
    monkeypatch.setattr(admission.asyncio, "sleep", sleep)
    context = _context(
        postgres_engine,
        provider_pool_id,
        events,
        capacity=(False, True),
        on_check=observe_lock,
    )
    async with AsyncExitStack() as stack:
        sandbox = await _enter(stack, context, waiting, events, on_create=observe_lock)
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
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    _, _, (task,) = _run(
        postgres_session,
        pool_id,
        [("cancelled", TaskStatus.PENDING, _ATTEMPT)],
    )
    capacity_started = asyncio.Event()
    hold_capacity = asyncio.Event()

    async def block_capacity() -> None:
        capacity_started.set()
        await hold_capacity.wait()

    async with AsyncExitStack() as stack:
        entering = asyncio.create_task(
            _enter(stack, _context(postgres_engine, provider_pool_id, [], on_check=block_capacity), task, [])
        )
        await capacity_started.wait()
        entering.cancel()
        with pytest.raises(asyncio.CancelledError):
            await entering

    async with store.PostgresPoolLock(postgres_engine, pool_id) as acquired:
        pass
    assert acquired is True


async def test_stale_creation_is_cleaned_before_cancellation(
    postgres_engine: Engine,
    postgres_session: Session,
) -> None:
    provider_pool_id = f"daytona:{uuid4()}"
    pool_id = store.queue_pool_id(provider_pool_id)
    _, _, (capacity_task, creation_task) = _run(
        postgres_session,
        pool_id,
        [
            ("stale-capacity", TaskStatus.PENDING, _ATTEMPT),
            ("stale-creation", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
        ],
        concurrency=2,
    )
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


async def test_setup_retry_reenters_fifo_before_competitor(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    harness_config: HarnessConfig,
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

    await task_execution.process_task(
        retrying,
        benchmark.start_benchmark_request(harness_config),
        cast(BenchmarkServiceClient, service),
        benchmark.id,
        retrying.task_id,
        harness_config,
        org,
        cast(SandboxProviderConfig, object()),
        context.provider,
        Semaphore(1),
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
