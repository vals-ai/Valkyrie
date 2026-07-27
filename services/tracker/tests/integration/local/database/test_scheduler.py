"""Tests for PostgreSQL scheduler admission.

Run: uv run pytest tests/integration/local/database/test_scheduler.py

Covers atomic claims, attempt fencing, advisory-lock retention, and recovery.
"""

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID, uuid4

from benchmark_service import ImageSource, Resources, Sandbox, SandboxProvider, SandboxSource
import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session, col, select

from tests.factories import make_task
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    Org,
    Task,
    TaskStatus,
)
import tracker.scheduler.admission as admission_module
import tracker.scheduler.store as store_module

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
) -> Callable[[], AbstractAsyncContextManager[Sandbox]]:
    @asynccontextmanager
    async def sandbox() -> AsyncGenerator[Sandbox]:
        events.append("create")
        if on_create is not None:
            await on_create()
        try:
            yield cast(Sandbox, object())
        finally:
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

    def test_claim_and_recovery_are_attempt_fenced(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
    ) -> None:
        org = Org(id=uuid4(), name=f"scheduler-claim-{uuid4()}")
        pool_id = store_module.queue_pool_id(f"daytona:{uuid4()}")
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
        abandoned = make_task(second_run, "abandoned", status=TaskStatus.BUILDING, started_at=_ATTEMPT)
        postgres_session.add_all([first, second, abandoned])
        postgres_session.commit()

        assert not store_module.claim_eligible_task(postgres_session, pool_id, second.id, second.started_at)
        assert not store_module.claim_eligible_task(
            postgres_session,
            pool_id,
            first.id,
            first.started_at + timedelta(microseconds=1),
        )
        assert store_module.claim_eligible_task(postgres_session, pool_id, first.id, first.started_at)

        recovered = store_module.reset_abandoned_builds(postgres_session, pool_id, _ATTEMPT)
        postgres_session.commit()

        with Session(postgres_engine) as assertion_session:
            persisted = {
                task.id: task
                for task in assertion_session.exec(
                    select(Task).where(col(Task.id).in_([first.id, second.id, abandoned.id]))
                ).all()
            }

        assert persisted[first.id].status == TaskStatus.PENDING
        assert persisted[first.id].started_at > _ATTEMPT
        assert persisted[second.id].status == TaskStatus.PENDING
        assert persisted[abandoned.id].status == TaskStatus.PENDING
        assert persisted[abandoned.id].started_at > _ATTEMPT
        assert recovered == 2


class TestPostgresAdmission:
    """Database-fenced provider admission."""

    async def test_holds_lock_and_keeps_claim_after_concurrency_decrease(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
    ) -> None:
        org = Org(id=uuid4(), name=f"scheduler-admission-{uuid4()}")
        provider_pool_id = f"daytona:{uuid4()}"
        pool_id = store_module.queue_pool_id(provider_pool_id)
        events: list[str] = []
        lock_observations: list[bool] = []
        postgres_session.add(org)
        postgres_session.flush()
        benchmark = _make_benchmark(
            postgres_session,
            org_id=org.id,
            name="admitted-run",
            pool_id=pool_id,
            concurrency=2,
        )
        task = make_task(benchmark, "admitted-task", started_at=_ATTEMPT)
        postgres_session.add(task)
        postgres_session.commit()

        async def observe_lock() -> None:
            async with store_module.PostgresPoolLock(postgres_engine, pool_id) as acquired:
                lock_observations.append(acquired)

        async def decrease_concurrency() -> None:
            await observe_lock()
            with Session(postgres_engine) as session:
                persisted_benchmark = session.get(Benchmark, benchmark.id)
                assert persisted_benchmark is not None
                persisted_benchmark.arguments = persisted_benchmark.arguments.model_copy(update={"concurrency": 1})
                session.add(persisted_benchmark)
                session.commit()

        context = admission_module.create_queue_context(
            engine=postgres_engine,
            poll_interval_seconds=0,
            provider=cast(
                SandboxProvider,
                MockProvider(provider_pool_id, events, on_check=observe_lock),
            ),
        )
        with pytest.raises(ValueError, match="does not support queued admission"):
            admission_module.create_queue_context(
                engine=postgres_engine,
                provider=cast(SandboxProvider, MockProvider(None, events)),
            )

        async with AsyncExitStack() as stack:
            sandbox = await admission_module.enter_queued_sandbox(
                stack=stack,
                context=context,
                task_row_id=task.id,
                expected_started_at=task.started_at,
                source=_source(),
                resources=_resources(),
                create=_sandbox_factory(events, on_create=decrease_concurrency),
            )

            with Session(postgres_engine) as assertion_session:
                persisted = assertion_session.get(Task, task.id)

            assert sandbox is not None
            assert persisted is not None
            assert persisted.status == TaskStatus.IN_PROGRESS
            assert events == ["capacity", "create"]
            assert lock_observations == [False, False]

        assert events == ["capacity", "create", "cleanup"]

    async def test_revalidates_exact_attempt_and_cleans_stale_creation(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
    ) -> None:
        org = Org(id=uuid4(), name=f"scheduler-revalidation-{uuid4()}")
        pool_id = store_module.queue_pool_id(f"daytona:{uuid4()}")
        postgres_session.add(org)
        postgres_session.flush()
        capacity_run = _make_benchmark(
            postgres_session,
            org_id=org.id,
            name="capacity-run",
            pool_id=pool_id,
            priority=0,
        )
        creation_run = _make_benchmark(
            postgres_session,
            org_id=org.id,
            name="creation-run",
            pool_id=pool_id,
            priority=1,
        )
        capacity_task = make_task(capacity_run, "capacity-task", started_at=_ATTEMPT)
        creation_task = make_task(creation_run, "creation-task", started_at=_ATTEMPT + timedelta(minutes=1))
        postgres_session.add_all([capacity_task, creation_task])
        postgres_session.commit()

        capacity_events: list[str] = []

        async def supersede_capacity_attempt() -> None:
            _update_task(
                postgres_engine,
                capacity_task.id,
                status=TaskStatus.STOPPED,
                started_at=_ATTEMPT + timedelta(microseconds=1),
            )

        capacity_context = admission_module.SandboxQueueContext(
            provider=cast(
                SandboxProvider,
                MockProvider(
                    "daytona:organization",
                    capacity_events,
                    on_check=supersede_capacity_attempt,
                ),
            ),
            pool_id=pool_id,
            engine=postgres_engine,
            poll_interval_seconds=0,
        )
        async with AsyncExitStack() as stack:
            stale_before_capacity = await admission_module.enter_queued_sandbox(
                stack=stack,
                context=capacity_context,
                task_row_id=capacity_task.id,
                expected_started_at=_ATTEMPT - timedelta(microseconds=1),
                source=_source(),
                resources=_resources(),
                create=_sandbox_factory(capacity_events),
            )
        async with AsyncExitStack() as stack:
            after_capacity = await admission_module.enter_queued_sandbox(
                stack=stack,
                context=capacity_context,
                task_row_id=capacity_task.id,
                expected_started_at=_ATTEMPT,
                source=_source(),
                resources=_resources(),
                create=_sandbox_factory(capacity_events),
            )

        creation_events: list[str] = []

        async def supersede_created_attempt() -> None:
            _update_task(
                postgres_engine,
                creation_task.id,
                status=TaskStatus.PENDING,
                started_at=creation_task.started_at + timedelta(microseconds=1),
            )

        creation_context = admission_module.SandboxQueueContext(
            provider=cast(SandboxProvider, MockProvider("daytona:organization", creation_events)),
            pool_id=pool_id,
            engine=postgres_engine,
            poll_interval_seconds=0,
        )
        async with AsyncExitStack() as stack:
            after_creation = await admission_module.enter_queued_sandbox(
                stack=stack,
                context=creation_context,
                task_row_id=creation_task.id,
                expected_started_at=creation_task.started_at,
                source=_source(),
                resources=_resources(),
                create=_sandbox_factory(creation_events, on_create=supersede_created_attempt),
            )

            assert creation_events == ["capacity", "create", "cleanup"]

        assert stale_before_capacity is None
        assert after_capacity is None
        assert capacity_events == ["capacity"]
        assert after_creation is None
        assert creation_events == ["capacity", "create", "cleanup"]
