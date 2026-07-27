"""Tests for PostgreSQL scheduler locking and recovery.

Run: uv run pytest tests/integration/local/database/test_scheduler.py

Covers provider-pool exclusion and abandoned build recovery in real PostgreSQL.
"""

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from datetime import datetime
from typing import cast
from uuid import UUID, uuid4

from benchmark_service import ImageSource, Resources, Sandbox, SandboxProvider, SandboxSource
import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session, col, select

from tests.factories import make_task
import tracker.scheduler.admission as admission_module
import tracker.scheduler.store as store_module
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    Org,
    Task,
    TaskStatus,
)

_FRESH_ATTEMPT = datetime(2026, 7, 27, 13)


class MockProvider:
    def __init__(
        self,
        pool_id: str | None,
        events: list[str],
        *,
        on_check: Callable[[], None] | None = None,
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
            self._on_check()

        return True


def _make_benchmark(
    session: Session,
    *,
    org_id: UUID,
    name: str,
    pool_id: str,
) -> Benchmark:
    benchmark = Benchmark(
        org_id=org_id,
        name=name,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name=name, install_cmd="true", run_cmd="true"),
            concurrency=1,
            priority=3,
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
    on_create: Callable[[], None] | None = None,
) -> Callable[[], AbstractAsyncContextManager[Sandbox]]:
    @asynccontextmanager
    async def sandbox() -> AsyncGenerator[Sandbox]:
        events.append("create")
        if on_create is not None:
            on_create()
        try:
            yield cast(Sandbox, object())
        finally:
            events.append("cleanup")

    return sandbox


def _stop_benchmark(engine: Engine, benchmark_id: UUID) -> None:
    with Session(engine) as session:
        benchmark = session.get(Benchmark, benchmark_id)
        assert benchmark is not None
        benchmark.status = BenchmarkStatus.STOPPING
        session.add(benchmark)
        session.commit()


class TestPostgresScheduler:
    """Cross-connection exclusion and recovery."""

    async def test_pool_lock_serializes_contenders(self, postgres_engine: Engine) -> None:
        pool_id = store_module.queue_pool_id("daytona:organization")
        other_pool_id = store_module.queue_pool_id("daytona:other")

        async with admission_module.PostgresPoolLock(postgres_engine, pool_id) as first_acquired:
            async with admission_module.PostgresPoolLock(postgres_engine, pool_id) as second_acquired:
                pass
            async with admission_module.PostgresPoolLock(postgres_engine, other_pool_id) as other_pool_acquired:
                pass

        async with admission_module.PostgresPoolLock(postgres_engine, pool_id) as reacquired:
            pass

        assert first_acquired is True
        assert second_acquired is False
        assert other_pool_acquired is True
        assert reacquired is True

    async def test_locked_recovery_refreshes_only_abandoned_pool_builds(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
    ) -> None:
        org = Org(id=uuid4(), name=f"scheduler-recovery-{uuid4()}")
        pool_id = store_module.queue_pool_id("daytona:organization")

        postgres_session.add(org)
        postgres_session.flush()

        queued_run = _make_benchmark(
            postgres_session,
            org_id=org.id,
            name="queued-run",
            pool_id=pool_id,
        )
        other_pool_run = _make_benchmark(
            postgres_session,
            org_id=org.id,
            name="other-pool-run",
            pool_id=store_module.queue_pool_id("daytona:other"),
        )
        abandoned = make_task(
            queued_run,
            "abandoned-build",
            status=TaskStatus.BUILDING,
            started_at=datetime(2026, 7, 27, 12),
        )
        active = make_task(
            queued_run,
            "active-task",
            status=TaskStatus.IN_PROGRESS,
            started_at=datetime(2026, 7, 27, 12),
        )
        other_pool_build = make_task(
            other_pool_run,
            "other-pool-build",
            status=TaskStatus.BUILDING,
            started_at=datetime(2026, 7, 27, 12),
        )

        postgres_session.add_all([abandoned, active, other_pool_build])
        postgres_session.commit()

        async with admission_module.PostgresPoolLock(postgres_engine, pool_id):
            with Session(postgres_engine) as recovery_session:
                recovered = store_module.reset_abandoned_builds(recovery_session, pool_id, _FRESH_ATTEMPT)
                recovery_session.commit()

        with Session(postgres_engine) as assertion_session:
            persisted_tasks = {
                task.task_id: task
                for task in assertion_session.exec(
                    select(Task).where(col(Task.id).in_([abandoned.id, active.id, other_pool_build.id]))
                ).all()
            }

        assert recovered == 1
        assert persisted_tasks["abandoned-build"].status == TaskStatus.PENDING
        assert persisted_tasks["abandoned-build"].started_at == _FRESH_ATTEMPT
        assert persisted_tasks["active-task"].status == TaskStatus.IN_PROGRESS
        assert persisted_tasks["other-pool-build"].status == TaskStatus.BUILDING


class TestPostgresAdmission:
    """Database-fenced provider admission."""

    def test_queue_context_hashes_provider_pool_and_rejects_unsupported_provider(
        self,
        postgres_engine: Engine,
    ) -> None:
        events: list[str] = []
        provider = MockProvider("daytona:organization", events)

        context = admission_module.create_queue_context(
            engine=postgres_engine,
            provider=cast(SandboxProvider, provider),
            poll_interval_seconds=0,
        )

        assert context.pool_id == store_module.queue_pool_id("daytona:organization")
        assert context.engine is postgres_engine

        with pytest.raises(ValueError, match="does not support queued admission"):
            admission_module.create_queue_context(
                engine=postgres_engine,
                provider=cast(SandboxProvider, MockProvider(None, events)),
            )

    async def test_admission_transitions_through_building_and_defers_cleanup(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
    ) -> None:
        org = Org(id=uuid4(), name=f"scheduler-admission-{uuid4()}")
        pool_id = store_module.queue_pool_id("daytona:organization")
        events: list[str] = []

        postgres_session.add(org)
        postgres_session.flush()
        benchmark = _make_benchmark(
            postgres_session,
            org_id=org.id,
            name="admitted-run",
            pool_id=pool_id,
        )
        task = make_task(benchmark, "admitted-task", started_at=datetime(2026, 7, 27, 12))
        postgres_session.add(task)
        postgres_session.commit()

        context = admission_module.SandboxQueueContext(
            provider=cast(SandboxProvider, MockProvider("daytona:organization", events)),
            pool_id=pool_id,
            engine=postgres_engine,
            poll_interval_seconds=0,
        )

        async with AsyncExitStack() as stack:
            sandbox = await admission_module.enter_queued_sandbox(
                stack=stack,
                context=context,
                task_row_id=task.id,
                source=_source(),
                resources=_resources(),
                create=_sandbox_factory(events),
            )

            with Session(postgres_engine) as assertion_session:
                persisted = assertion_session.get(Task, task.id)

            assert sandbox is not None
            assert persisted is not None
            assert persisted.status == TaskStatus.IN_PROGRESS
            assert events == ["capacity", "create"]

        assert events == ["capacity", "create", "cleanup"]

    async def test_admission_revalidates_after_capacity_and_creation(
        self,
        postgres_engine: Engine,
        postgres_session: Session,
    ) -> None:
        org = Org(id=uuid4(), name=f"scheduler-revalidation-{uuid4()}")
        pool_id = store_module.queue_pool_id("daytona:organization")

        postgres_session.add(org)
        postgres_session.flush()
        capacity_benchmark = _make_benchmark(
            postgres_session,
            org_id=org.id,
            name="capacity-revalidation-run",
            pool_id=pool_id,
        )
        creation_benchmark = _make_benchmark(
            postgres_session,
            org_id=org.id,
            name="creation-revalidation-run",
            pool_id=pool_id,
        )
        stopped_after_capacity = make_task(
            capacity_benchmark,
            "stopped-after-capacity",
            started_at=datetime(2026, 7, 27, 12),
        )
        stopped_after_creation = make_task(
            creation_benchmark,
            "stopped-after-creation",
            started_at=datetime(2026, 7, 27, 12, 1),
        )
        postgres_session.add_all([stopped_after_capacity, stopped_after_creation])
        postgres_session.commit()

        capacity_events: list[str] = []
        capacity_context = admission_module.SandboxQueueContext(
            provider=cast(
                SandboxProvider,
                MockProvider(
                    "daytona:organization",
                    capacity_events,
                    on_check=lambda: _stop_benchmark(postgres_engine, capacity_benchmark.id),
                ),
            ),
            pool_id=pool_id,
            engine=postgres_engine,
            poll_interval_seconds=0,
        )

        async with AsyncExitStack() as stack:
            after_capacity = await asyncio.wait_for(
                admission_module.enter_queued_sandbox(
                    stack=stack,
                    context=capacity_context,
                    task_row_id=stopped_after_capacity.id,
                    source=_source(),
                    resources=_resources(),
                    create=_sandbox_factory(capacity_events),
                ),
                timeout=1,
            )

        creation_events: list[str] = []
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
                task_row_id=stopped_after_creation.id,
                source=_source(),
                resources=_resources(),
                create=_sandbox_factory(
                    creation_events,
                    on_create=lambda: _stop_benchmark(postgres_engine, creation_benchmark.id),
                ),
            )

            assert creation_events == ["capacity", "create"]

        assert after_capacity is None
        assert capacity_events == ["capacity"]
        assert after_creation is None
        assert creation_events == ["capacity", "create", "cleanup"]
