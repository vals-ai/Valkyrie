"""Tests for PostgreSQL scheduler locking and recovery.

Run: uv run pytest tests/integration/local/database/test_scheduler.py

Covers provider-pool exclusion and abandoned build recovery in real PostgreSQL.
"""

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy.engine import Engine
from sqlmodel import Session, col, select

from tests.factories import make_task
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkArguments, Org, Task, TaskStatus
import tracker.scheduler.store as store_module

_FRESH_ATTEMPT = datetime(2026, 7, 27, 13)


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


class TestPostgresScheduler:
    """Cross-connection exclusion and abandoned-build recovery."""

    async def test_pool_lock_isolates_ownership(self, postgres_engine: Engine) -> None:
        pool_id = store_module.queue_pool_id("daytona:organization")
        other_pool_id = store_module.queue_pool_id("daytona:other")

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

        async with store_module.PostgresPoolLock(postgres_engine, pool_id) as acquired:
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

        assert acquired is True
        assert recovered == 1
        assert persisted_tasks["abandoned-build"].status == TaskStatus.PENDING
        assert persisted_tasks["abandoned-build"].started_at == _FRESH_ATTEMPT
        assert persisted_tasks["active-task"].status == TaskStatus.IN_PROGRESS
        assert persisted_tasks["other-pool-build"].status == TaskStatus.BUILDING
