"""Tests for PostgreSQL scheduler admission.

Run: uv run pytest tests/integration/local/database/test_scheduler.py

Covers atomic claims, attempt fencing, advisory-lock retention, and recovery.
"""

import asyncio
from asyncio import Semaphore
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta
from typing import cast
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
import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session
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
import tracker.scheduler.admission as admission_module
import tracker.scheduler.store as store_module
from tracker.types import HarnessConfig
from tracker.utils import task_execution as task_execution_module

_ATTEMPT = datetime(2026, 7, 27, 12)


class MockProvider:
    """Provider capacity boundary with an optional state-changing hook."""

    def __init__(
        self,
        pool_id: str | None,
        events: list[str],
        *,
        on_check: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._pool_id = pool_id
        self._events = events
        self._on_check = on_check

    @property
    def admission_pool_id(self) -> str | None:
        return self._pool_id

    async def check_admission(self, _source: SandboxSource, _resources: Resources) -> bool:
        self._events.append("capacity")
        if self._on_check is not None:
            await self._on_check()

        return True


def _make_benchmark(
    session: Session,
    *,
    org_id: UUID,
    name: str,
    pool_id: str,
    concurrency: int = 1,
    priority: int = 3,
) -> Benchmark:
    benchmark = Benchmark(
        org_id=org_id,
        name=name,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name=name, install_cmd="true", run_cmd="true"),
            concurrency=concurrency,
            priority=priority,
            queue_pool_id=pool_id,
        ),
    )
    session.add(benchmark)
    session.flush()

    return benchmark


def _source() -> SandboxSource:
    return ImageSource(image="scheduler-test-image")


def _resources() -> Resources:
    return Resources(vcpu=1, memory=2, disk=3)


def _sandbox_factory(
    events: list[str],
    *,
    on_create: Callable[[], Awaitable[None]] | None = None,
    on_cleanup: Callable[[], Awaitable[None]] | None = None,
    sandbox_name: str = "scheduler-sandbox",
) -> Callable[[], AbstractAsyncContextManager[Sandbox]]:
    @asynccontextmanager
    async def sandbox() -> AsyncGenerator[Sandbox]:
        events.append("create")
        if on_create is not None:
            await on_create()
        sandbox = Mock()
        sandbox.id = f"sandbox-{len(events)}"
        sandbox.name = sandbox_name
        try:
            yield cast(Sandbox, sandbox)
        finally:
            if on_cleanup is not None:
                await on_cleanup()
            events.append("cleanup")

    return sandbox


def _update_task(
    engine: Engine,
    task_row_id: UUID,
    *,
    status: TaskStatus,
    started_at: datetime,
) -> None:
    with Session(engine) as session:
        task = session.get(Task, task_row_id)
        assert task is not None
        task.status = status
        task.started_at = started_at
        session.add(task)
        session.commit()


def _make_admission_run(
    session: Session,
    *,
    pool_id: str,
    task_specs: Sequence[tuple[str, TaskStatus, datetime]],
    concurrency: int = 1,
) -> tuple[Benchmark, list[Task]]:
    org = Org(id=uuid4(), name=f"scheduler-admission-{uuid4()}")
    session.add(org)
    session.flush()
    benchmark = _make_benchmark(
        session,
        org_id=org.id,
        name=f"admission-run-{uuid4()}",
        pool_id=pool_id,
        concurrency=concurrency,
    )
    tasks = [
        make_task(benchmark, task_id, status=status, started_at=started_at)
        for task_id, status, started_at in task_specs
    ]
    session.add_all(tasks)
    session.commit()

    return benchmark, tasks


def _queue_context(
    engine: Engine,
    provider_pool_id: str,
    events: list[str],
    *,
    pool_id: str | None = None,
    on_check: Callable[[], Awaitable[None]] | None = None,
) -> admission_module.SandboxQueueContext:
    return admission_module.SandboxQueueContext(
        provider=cast(
            SandboxProvider,
            MockProvider(provider_pool_id, events, on_check=on_check),
        ),
        pool_id=pool_id or store_module.queue_pool_id(provider_pool_id),
        engine=engine,
        poll_interval_seconds=0,
    )


async def _enter(
    stack: AsyncExitStack,
    context: admission_module.SandboxQueueContext,
    task: Task,
    events: list[str],
    *,
    expected_started_at: datetime | None = None,
    on_create: Callable[[], Awaitable[None]] | None = None,
    on_cleanup: Callable[[], Awaitable[None]] | None = None,
) -> Sandbox | None:
    return await admission_module.enter_queued_sandbox(
        stack=stack,
        context=context,
        task_row_id=task.id,
        expected_started_at=expected_started_at or task.started_at,
        source=_source(),
        resources=_resources(),
        create=_sandbox_factory(events, on_create=on_create, on_cleanup=on_cleanup),
    )


def _set_concurrency(engine: Engine, benchmark_id: UUID, concurrency: int) -> None:
    with Session(engine) as session:
        benchmark = session.get(Benchmark, benchmark_id)
        assert benchmark is not None
        benchmark.arguments = benchmark.arguments.model_copy(update={"concurrency": concurrency})
        session.add(benchmark)
        session.commit()


class TestPostgresScheduler:
    """Cross-connection exclusion, atomic selection, and abandoned recovery."""

    async def test_pool_lock_isolates_ownership(self, postgres_engine: Engine) -> None:
        pool_id = store_module.queue_pool_id(f"daytona:{uuid4()}")
        other_pool_id = store_module.queue_pool_id(f"daytona:{uuid4()}")

        async with store_module.PostgresPoolLock(postgres_engine, pool_id) as first_acquired:
            async with store_module.PostgresPoolLock(postgres_engine, pool_id) as second_acquired:
                pass
            async with store_module.PostgresPoolLock(postgres_engine, other_pool_id) as other_pool_acquired:
                pass

        async with store_module.PostgresPoolLock(postgres_engine, pool_id) as reacquired:
            pass

        assert first_acquired is True
        assert second_acquired is False
        assert other_pool_acquired is True
        assert reacquired is True

    async def test_claim_and_building_only_recovery_are_attempt_fenced(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
    ) -> None:
        org = Org(id=uuid4(), name=f"scheduler-claim-{uuid4()}")
        provider_pool_id = f"daytona:{uuid4()}"
        pool_id = store_module.queue_pool_id(provider_pool_id)
        events: list[str] = []
        postgres_session.add(org)
        postgres_session.flush()
        first_run = _make_benchmark(
            postgres_session,
            org_id=org.id,
            name="first-run",
            pool_id=pool_id,
            priority=0,
        )
        second_run = _make_benchmark(
            postgres_session,
            org_id=org.id,
            name="second-run",
            pool_id=pool_id,
            priority=1,
        )
        first = make_task(first_run, "first", started_at=_ATTEMPT)
        second = make_task(second_run, "second", started_at=_ATTEMPT)
        postgres_session.add_all([first, second])
        postgres_session.commit()

        assert not store_module.claim_eligible_task(postgres_session, pool_id, second.id, second.started_at)
        assert not store_module.claim_eligible_task(
            postgres_session,
            pool_id,
            first.id,
            first.started_at + timedelta(microseconds=1),
        )
        assert store_module.claim_eligible_task(postgres_session, pool_id, first.id, first.started_at)

        second.status = TaskStatus.STOPPED
        postgres_session.add(second)
        postgres_session.commit()
        context = _queue_context(postgres_engine, provider_pool_id, events)

        await admission_module.recover_queued_pool(context)

        with Session(postgres_engine) as assertion_session:
            recovered = assertion_session.get(Task, first.id)
            assert recovered is not None
            recovered_attempt = recovered.started_at
            assert recovered.status == TaskStatus.PENDING
            assert recovered_attempt > _ATTEMPT

        async with AsyncExitStack() as stack:
            sandbox = await _enter(
                stack,
                context,
                first,
                events,
                expected_started_at=recovered_attempt,
            )

            with Session(postgres_engine) as assertion_session:
                admitted = assertion_session.get(Task, first.id)

            assert sandbox is not None
            assert admitted is not None
            assert admitted.status == TaskStatus.IN_PROGRESS

        assert events == ["capacity", "create", "cleanup"]


class TestPostgresAdmission:
    """Database-fenced provider admission."""

    async def test_lock_retention_and_dynamic_concurrency(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        provider_pool_id = f"daytona:{uuid4()}"
        pool_id = store_module.queue_pool_id(provider_pool_id)
        first_events: list[str] = []
        second_events: list[str] = []
        lock_observations: list[bool] = []
        benchmark, (first, second) = _make_admission_run(
            postgres_session,
            pool_id=pool_id,
            concurrency=2,
            task_specs=[
                ("first", TaskStatus.PENDING, _ATTEMPT),
                ("second", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
            ],
        )

        async def observe_lock() -> None:
            async with store_module.PostgresPoolLock(postgres_engine, pool_id) as acquired:
                lock_observations.append(acquired)

        async def decrease_concurrency() -> None:
            await observe_lock()
            _set_concurrency(postgres_engine, benchmark.id, 1)

        first_context = admission_module.create_queue_context(
            engine=postgres_engine,
            poll_interval_seconds=0,
            provider=cast(
                SandboxProvider,
                MockProvider(provider_pool_id, first_events, on_check=observe_lock),
            ),
        )
        with pytest.raises(ValueError, match="does not support queued admission"):
            admission_module.create_queue_context(
                engine=postgres_engine,
                provider=cast(SandboxProvider, MockProvider(None, first_events)),
            )

        blocked_poll = asyncio.Event()
        retry_poll = asyncio.Event()

        async def controlled_backoff(_seconds: float) -> None:
            blocked_poll.set()
            await retry_poll.wait()

        second_context = _queue_context(postgres_engine, provider_pool_id, second_events)
        async with AsyncExitStack() as first_stack, AsyncExitStack() as second_stack:
            first_sandbox = await _enter(
                first_stack,
                first_context,
                first,
                first_events,
                on_create=decrease_concurrency,
            )
            monkeypatch.setattr("tracker.scheduler.admission.asyncio.sleep", controlled_backoff)
            second_admission = asyncio.create_task(_enter(second_stack, second_context, second, second_events))
            await blocked_poll.wait()

            with Session(postgres_engine) as assertion_session:
                persisted_first = assertion_session.get(Task, first.id)
                blocked_second = assertion_session.get(Task, second.id)

            assert first_sandbox is not None
            assert persisted_first is not None
            assert persisted_first.status == TaskStatus.IN_PROGRESS
            assert blocked_second is not None
            assert blocked_second.status == TaskStatus.PENDING
            assert lock_observations == [False, False]
            assert second_events == []

            _set_concurrency(postgres_engine, benchmark.id, 2)
            retry_poll.set()

            second_sandbox = await second_admission

            with Session(postgres_engine) as assertion_session:
                admitted_second = assertion_session.get(Task, second.id)

            assert second_sandbox is not None
            assert admitted_second is not None
            assert admitted_second.status == TaskStatus.IN_PROGRESS
            assert second_events == ["capacity", "create"]

        assert first_events == ["capacity", "create", "cleanup"]
        assert second_events == ["capacity", "create", "cleanup"]

    async def test_provider_failure_and_cancellation_release_pool_lock(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
    ) -> None:
        provider_pool_id = f"daytona:{uuid4()}"
        pool_id = store_module.queue_pool_id(provider_pool_id)
        _, (task,) = _make_admission_run(
            postgres_session,
            pool_id=pool_id,
            task_specs=[("provider-release", TaskStatus.PENDING, _ATTEMPT)],
        )

        async def fail_capacity() -> None:
            raise RuntimeError("provider capacity failed")

        failure_context = _queue_context(
            postgres_engine,
            provider_pool_id,
            [],
            on_check=fail_capacity,
        )
        async with AsyncExitStack() as stack:
            with pytest.raises(RuntimeError, match="provider capacity failed"):
                await _enter(stack, failure_context, task, [])

        async with store_module.PostgresPoolLock(postgres_engine, pool_id) as acquired_after_failure:
            pass

        capacity_started = asyncio.Event()
        hold_capacity = asyncio.Event()

        async def block_capacity() -> None:
            capacity_started.set()
            await hold_capacity.wait()

        cancellation_context = _queue_context(
            postgres_engine,
            provider_pool_id,
            [],
            on_check=block_capacity,
        )
        async with AsyncExitStack() as stack:
            admission = asyncio.create_task(_enter(stack, cancellation_context, task, []))
            await capacity_started.wait()
            admission.cancel()

            with pytest.raises(asyncio.CancelledError):
                await admission

        async with store_module.PostgresPoolLock(postgres_engine, pool_id) as acquired_after_cancellation:
            pass

        assert acquired_after_failure is True
        assert acquired_after_cancellation is True

    async def test_revalidates_exact_attempt_and_cleans_stale_creation(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
    ) -> None:
        pool_id = store_module.queue_pool_id(f"daytona:{uuid4()}")
        _, (capacity_task, creation_task) = _make_admission_run(
            postgres_session,
            pool_id=pool_id,
            task_specs=[
                ("capacity-task", TaskStatus.PENDING, _ATTEMPT),
                ("creation-task", TaskStatus.PENDING, _ATTEMPT + timedelta(minutes=1)),
            ],
        )

        capacity_events: list[str] = []

        async def supersede_capacity_attempt() -> None:
            _update_task(
                postgres_engine,
                capacity_task.id,
                status=TaskStatus.STOPPED,
                started_at=_ATTEMPT + timedelta(microseconds=1),
            )

        capacity_context = _queue_context(
            postgres_engine,
            "daytona:organization",
            capacity_events,
            pool_id=pool_id,
            on_check=supersede_capacity_attempt,
        )
        async with AsyncExitStack() as stack:
            after_capacity = await _enter(stack, capacity_context, capacity_task, capacity_events)

        creation_events: list[str] = []
        cleanup_started = asyncio.Event()
        finish_cleanup = asyncio.Event()

        async def supersede_created_attempt() -> None:
            _update_task(
                postgres_engine,
                creation_task.id,
                status=TaskStatus.PENDING,
                started_at=creation_task.started_at + timedelta(microseconds=1),
            )

        async def block_cleanup() -> None:
            cleanup_started.set()
            await finish_cleanup.wait()

        creation_context = _queue_context(
            postgres_engine,
            "daytona:organization",
            creation_events,
            pool_id=pool_id,
        )
        async with AsyncExitStack() as stack:
            stale_creation = asyncio.create_task(
                _enter(
                    stack,
                    creation_context,
                    creation_task,
                    creation_events,
                    on_create=supersede_created_attempt,
                    on_cleanup=block_cleanup,
                )
            )
            await cleanup_started.wait()
            stale_creation.cancel()
            barrier = asyncio.get_running_loop().create_future()
            asyncio.get_running_loop().call_soon(barrier.set_result, None)
            await barrier
            cleanup_outlived_cancellation = not stale_creation.done()
            finish_cleanup.set()

            with pytest.raises(asyncio.CancelledError):
                await stale_creation

        assert after_capacity is None
        assert capacity_events == ["capacity"]
        assert cleanup_outlived_cancellation is True
        assert creation_events == ["capacity", "create", "cleanup"]

    async def test_setup_retry_reenters_postgres_fifo_ahead_of_competitor(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
        monkeypatch: pytest.MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        provider_pool_id = f"daytona:{uuid4()}"
        pool_id = store_module.queue_pool_id(provider_pool_id)
        events: list[str] = []
        benchmark, (retrying, competitor) = _make_admission_run(
            postgres_session,
            pool_id=pool_id,
            task_specs=[
                ("retrying", TaskStatus.PENDING, _ATTEMPT),
                ("competitor", TaskStatus.PENDING, _ATTEMPT + timedelta(microseconds=1)),
            ],
        )
        dormant_evaluation = make_task(
            benchmark,
            "dormant-evaluation",
            status=TaskStatus.EVALUATING,
            started_at=retrying.started_at + timedelta(microseconds=2),
        )
        postgres_session.add(dormant_evaluation)
        postgres_session.commit()
        org = postgres_session.get(Org, benchmark.org_id)
        assert org is not None
        admission_states: list[tuple[TaskStatus, datetime, TaskStatus]] = []

        async def observe_admission() -> None:
            with Session(postgres_engine) as assertion_session:
                durable_retry = assertion_session.get(Task, retrying.id)
                durable_competitor = assertion_session.get(Task, competitor.id)
            assert durable_retry is not None
            assert durable_competitor is not None
            admission_states.append(
                (
                    durable_retry.status,
                    durable_retry.started_at,
                    durable_competitor.status,
                )
            )

        context = _queue_context(
            postgres_engine,
            provider_pool_id,
            events,
            on_check=observe_admission,
        )
        sandbox_names: list[str] = []

        def create_sandbox(*_args: object, **kwargs: object) -> AbstractAsyncContextManager[Sandbox]:
            sandbox_name = cast(str, kwargs["sandbox_name"])
            sandbox_names.append(sandbox_name)
            return _sandbox_factory(events, sandbox_name=sandbox_name)()

        benchmark_service = AsyncMock(spec=BenchmarkServiceClient)
        benchmark_service.retrieve_task.return_value = RetrieveTaskResponse(
            source=_source(),
            problem_path="/tmp/problem.txt",
            cwd="/testbed",
            resources=_resources(),
        )
        benchmark_service.setup_task.return_value = None
        benchmark_service.evaluate_instance.return_value = {"status": "success", "score": 1.0}
        request = benchmark.start_benchmark_request(harness_config)
        retryable_process_task = getattr(task_execution_module, "_process_task_attempt")
        monkeypatch.setattr(retryable_process_task.retry, "wait", wait_none())
        monkeypatch.setattr(task_execution_module, "engine", postgres_engine)
        monkeypatch.setattr(task_execution_module, "buffer_logs", Mock())
        monkeypatch.setattr(task_execution_module, "create_sandbox", create_sandbox)
        monkeypatch.setattr(
            task_execution_module,
            "upload_agent_artifacts",
            AsyncMock(side_effect=[SandboxSetupError("retry sandbox setup"), None]),
        )
        monkeypatch.setattr(task_execution_module, "run_agent", AsyncMock(return_value=(None, 0.0)))

        await task_execution_module.process_task(
            retrying,
            request,
            cast(BenchmarkServiceClient, benchmark_service),
            benchmark.id,
            retrying.task_id,
            harness_config,
            org,
            sandbox_provider_config=cast(SandboxProviderConfig, object()),
            sandbox_provider=context.provider,
            creation_semaphore=Semaphore(1),
            queue_context=context,
        )

        with Session(postgres_engine) as assertion_session:
            durable_competitor = assertion_session.get(Task, competitor.id)

        assert durable_competitor is not None
        assert durable_competitor.status == TaskStatus.PENDING
        assert admission_states == [
            (TaskStatus.PENDING, retrying.started_at, TaskStatus.PENDING),
            (TaskStatus.PENDING, retrying.started_at, TaskStatus.PENDING),
        ]
        assert sandbox_names[0] == sandbox_names[1]
        assert retrying.id.hex in sandbox_names[0]
