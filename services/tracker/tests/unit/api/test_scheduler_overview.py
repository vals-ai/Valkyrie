"""Tests for PostgreSQL-backed scheduler overview reads."""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tests.utils import TEST_ORG_ID
from tracker.api.scheduler_overview import read_scheduler_overview
from tracker.database.models import Benchmark, BenchmarkStatus, Org, Task, TaskStatus


def _queue(benchmark: Benchmark, *, pool_id: str, priority: int) -> Benchmark:
    benchmark.arguments = benchmark.arguments.model_copy(update={"priority": priority, "queue_pool_id": pool_id})

    return benchmark


async def test_reports_org_queued_rows_in_priority_fifo_order(database_session: Session) -> None:
    now = datetime(2026, 7, 17, 12, tzinfo=UTC)
    pool_a, pool_b = "pool_a", "pool_b"
    other_org = Org(id=uuid4(), name="other")
    urgent_run = _queue(make_benchmark(name="urgent"), pool_id=pool_a, priority=0)
    second_run = _queue(make_benchmark(name="second"), pool_id=pool_b, priority=1)
    fifo_run = _queue(make_benchmark(name="fifo"), pool_id=pool_a, priority=3)
    direct = make_benchmark(name="direct")
    finished = _queue(make_benchmark(name="finished", status=BenchmarkStatus.FINISHED), pool_id=pool_a, priority=0)
    stopping = _queue(make_benchmark(name="stopping", status=BenchmarkStatus.STOPPING), pool_id=pool_a, priority=3)
    foreign = _queue(make_benchmark(name="foreign", org_id=other_org.id), pool_id=pool_a, priority=0)

    def task(
        benchmark: Benchmark,
        task_id: str,
        minutes: int,
        status: TaskStatus = TaskStatus.PENDING,
    ) -> Task:
        return make_task(benchmark, task_id, status=status, started_at=now - timedelta(minutes=minutes))

    foreign_first = task(foreign, "foreign-first", 20)
    urgent = task(urgent_run, "urgent", 15)
    second = task(second_run, "second", 10)
    fifo_first = task(fifo_run, "fifo-first", 5)
    fifo_first.id = UUID(int=1)
    fifo_second = task(fifo_run, "fifo-second", 5)
    fifo_second.id = UUID(int=2)
    stopping_active = task(stopping, "stopping-active", 32, TaskStatus.IN_PROGRESS)
    building = task(urgent_run, "building", 30, TaskStatus.BUILDING)
    running = task(second_run, "running", 25, TaskStatus.IN_PROGRESS)
    evaluating = task(fifo_run, "evaluating", 20, TaskStatus.EVALUATING)
    excluded = [
        task(finished, "finished-waiting", 25),
        task(finished, "finished-active", 35, TaskStatus.BUILDING),
        task(direct, "direct-waiting", 25),
        task(direct, "direct-active", 0, TaskStatus.IN_PROGRESS),
        task(foreign, "foreign-active", 0, TaskStatus.BUILDING),
    ]
    database_session.add_all(
        [
            other_org,
            urgent_run,
            second_run,
            fifo_run,
            direct,
            finished,
            stopping,
            foreign,
            foreign_first,
            urgent,
            second,
            fifo_first,
            fifo_second,
            stopping_active,
            building,
            running,
            evaluating,
            *excluded,
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
