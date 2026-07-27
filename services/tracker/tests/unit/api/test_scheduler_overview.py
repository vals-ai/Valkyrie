"""Tests for PostgreSQL-backed scheduler overview reads.

Run: uv run pytest tests/unit/api/test_scheduler_overview.py

Covers queued task ordering, pool positions, and active status counts.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tests.utils import TEST_ORG_ID
from tracker.api.scheduler_overview import read_scheduler_overview
from tracker.database.models import Benchmark, BenchmarkStatus, Org, TaskStatus


def _queue(benchmark: Benchmark, *, pool_id: str, priority: int) -> Benchmark:
    benchmark.arguments = benchmark.arguments.model_copy(update={"priority": priority, "queue_pool_id": pool_id})

    return benchmark


class TestReadSchedulerOverview:
    """PostgreSQL-backed scheduler overview behavior."""

    async def test_reports_org_queued_rows_in_priority_fifo_order(self, database_session: Session) -> None:
        now = datetime(2026, 7, 17, 12, tzinfo=UTC)
        pool_a, pool_b = "pool_a", "pool_b"
        other_org = Org(id=uuid4(), name="other")
        queued_urgent = _queue(make_benchmark(name="urgent"), pool_id=pool_a, priority=0)
        queued_second = _queue(make_benchmark(name="second"), pool_id=pool_b, priority=1)
        queued_fifo = _queue(make_benchmark(name="fifo"), pool_id=pool_a, priority=3)
        direct = make_benchmark(name="direct")
        finished = _queue(
            make_benchmark(name="finished", status=BenchmarkStatus.FINISHED),
            pool_id=pool_a,
            priority=0,
        )
        stopping = _queue(
            make_benchmark(name="stopping", status=BenchmarkStatus.STOPPING),
            pool_id=pool_a,
            priority=3,
        )
        foreign = _queue(make_benchmark(name="foreign", org_id=other_org.id), pool_id=pool_a, priority=0)

        foreign_first = make_task(foreign, "foreign-first", started_at=now - timedelta(minutes=20))
        finished_waiting = make_task(finished, "finished-waiting", started_at=now - timedelta(minutes=25))
        finished_active = make_task(
            finished,
            "finished-active",
            status=TaskStatus.BUILDING,
            started_at=now - timedelta(minutes=35),
        )
        stopping_active = make_task(
            stopping,
            "stopping-active",
            status=TaskStatus.IN_PROGRESS,
            started_at=now - timedelta(minutes=32),
        )
        urgent = make_task(queued_urgent, "urgent", started_at=now - timedelta(minutes=15))
        second = make_task(queued_second, "second", started_at=now - timedelta(minutes=10))
        fifo_first = make_task(queued_fifo, "fifo-first", started_at=now - timedelta(minutes=5))
        fifo_first.id = UUID("00000000-0000-0000-0000-000000000001")
        fifo_second = make_task(queued_fifo, "fifo-second", started_at=now - timedelta(minutes=5))
        fifo_second.id = UUID("00000000-0000-0000-0000-000000000002")
        direct_waiting = make_task(direct, "direct-waiting", started_at=now - timedelta(minutes=25))
        building = make_task(
            queued_urgent, "building", status=TaskStatus.BUILDING, started_at=now - timedelta(minutes=30)
        )
        running = make_task(
            queued_second, "running", status=TaskStatus.IN_PROGRESS, started_at=now - timedelta(minutes=25)
        )
        evaluating = make_task(
            queued_fifo, "evaluating", status=TaskStatus.EVALUATING, started_at=now - timedelta(minutes=20)
        )
        direct_active = make_task(direct, "direct-active", status=TaskStatus.IN_PROGRESS)
        foreign_active = make_task(foreign, "foreign-active", status=TaskStatus.BUILDING)
        database_session.add_all(
            [
                other_org,
                queued_urgent,
                queued_second,
                queued_fifo,
                direct,
                finished,
                stopping,
                foreign,
                foreign_first,
                finished_waiting,
                finished_active,
                stopping_active,
                urgent,
                second,
                fifo_first,
                fifo_second,
                direct_waiting,
                building,
                running,
                evaluating,
                direct_active,
                foreign_active,
            ]
        )
        database_session.commit()

        overview = read_scheduler_overview(
            session=database_session,
            org_id=TEST_ORG_ID,
            now=now,
            waiting_limit=3,
            active_limit=3,
        )

        assert overview.observed_at == now
        assert overview.summary.model_dump() == {"waiting": 4, "building": 1, "in_progress": 2, "evaluating": 1}
        assert [(pool.pool_id, pool.waiting) for pool in overview.pools] == [(pool_a, 3), (pool_b, 1)]
        assert [
            (entry.external_task_id, entry.pool_id, entry.position, entry.priority, entry.enqueued_at)
            for entry in overview.waiting_entries
        ] == [
            (urgent.task_id, pool_a, 2, 0, urgent.started_at),
            (second.task_id, pool_b, 1, 1, second.started_at),
            (fifo_first.task_id, pool_a, 3, 3, fifo_first.started_at),
        ]
        assert [(entry.external_task_id, entry.status) for entry in overview.active_entries] == [
            (stopping_active.task_id, TaskStatus.IN_PROGRESS),
            (building.task_id, TaskStatus.BUILDING),
            (running.task_id, TaskStatus.IN_PROGRESS),
        ]
        assert overview.waiting_capped
        assert overview.active_capped
