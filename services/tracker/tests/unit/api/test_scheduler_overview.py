from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

import pytest
from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tests.utils import TEST_ORG_ID
from tracker.api.scheduler_overview import read_scheduler_overview
from tracker.database.models import Benchmark, Org, Task, TaskStatus
from tracker.scheduler.gate import QueueSnapshotEntry, RedisQueueGate


class FakeGate:
    def __init__(self) -> None:
        self.entries: list[QueueSnapshotEntry] = []

    async def snapshot(self) -> list[QueueSnapshotEntry]:
        return self.entries


def _queue(benchmark: Benchmark) -> Benchmark:
    benchmark.arguments = benchmark.arguments.model_copy(update={"priority": 3})
    return benchmark


@pytest.mark.asyncio
async def test_overview_reports_mixed_org_scoped_live_snapshot(database_session: Session) -> None:
    now = datetime(2026, 7, 17, 12, tzinfo=UTC)
    member_time = now - timedelta(days=30)
    pool_a, pool_b = "a" * 24, "b" * 24
    queued = _queue(make_benchmark(name="queued", started_by_email="alice@vals.ai"))
    direct = make_benchmark(name="direct")
    other_org = Org(id=uuid4(), name="other")
    other = _queue(make_benchmark(name="other", org_id=other_org.id))

    urgent = make_task(queued, "urgent")
    fifo_first = make_task(queued, "fifo-first")
    fifo_second = make_task(queued, "fifo-second")
    other_pool = make_task(queued, "other-pool")
    stale = make_task(queued, "stale")
    direct_waiting = make_task(direct, "direct-waiting")
    foreign_waiting = make_task(other, "foreign-waiting")
    building = make_task(queued, "building", status=TaskStatus.BUILDING, started_at=now - timedelta(minutes=30))
    running = make_task(queued, "running", status=TaskStatus.IN_PROGRESS, started_at=now - timedelta(minutes=20))
    evaluating = make_task(queued, "evaluating", status=TaskStatus.EVALUATING, started_at=now - timedelta(minutes=10))
    direct_active = make_task(direct, "direct-active", status=TaskStatus.IN_PROGRESS)
    foreign_active = make_task(other, "foreign-active", status=TaskStatus.BUILDING)
    database_session.add_all(
        [
            queued,
            direct,
            other_org,
            other,
            urgent,
            fifo_first,
            fifo_second,
            other_pool,
            stale,
            direct_waiting,
            foreign_waiting,
            building,
            running,
            evaluating,
            direct_active,
            foreign_active,
        ]
    )
    database_session.commit()

    gate = FakeGate()

    def ticket(
        pool: str,
        priority: int,
        benchmark: Benchmark,
        task: Task,
        scored_at: datetime,
        *,
        live: bool = True,
        encoded_at: datetime | None = None,
    ) -> None:
        if live:
            gate.entries.append(
                QueueSnapshotEntry(
                    pool_id=f"pool_{pool}",
                    task_key=f"{benchmark.id}:{task.id}:{(encoded_at or scored_at).isoformat()}",
                    priority=priority,
                    enqueued_at=scored_at,
                )
            )

    urgent_at = now - timedelta(minutes=2)
    tied_at = now - timedelta(minutes=5)
    other_pool_at = now - timedelta(minutes=3)
    ticket(pool_a, 0, other, foreign_waiting, now - timedelta(hours=3))
    ticket(pool_a, 0, direct, direct_waiting, now - timedelta(hours=2))
    ticket(pool_a, 0, queued, stale, now - timedelta(hours=1), live=False)
    ticket(pool_a, 0, queued, urgent, urgent_at, encoded_at=member_time)
    ticket(pool_a, 2, queued, fifo_first, tied_at)
    ticket(pool_a, 2, queued, fifo_second, tied_at)
    ticket(pool_b, 1, queued, other_pool, other_pool_at)

    overview = await read_scheduler_overview(
        gate=cast(RedisQueueGate, gate),
        session=database_session,
        org_id=TEST_ORG_ID,
        now=now,
        waiting_limit=3,
        active_limit=2,
    )

    assert overview.observed_at == now
    assert overview.summary.model_dump() == {"waiting": 4, "building": 1, "in_progress": 1, "evaluating": 1}
    assert [(pool.pool_id, pool.waiting) for pool in overview.pools] == [
        (f"pool_{pool_a}", 3),
        (f"pool_{pool_b}", 1),
    ]
    assert [
        (entry.external_task_id, entry.position, entry.priority, entry.enqueued_at)
        for entry in overview.waiting_entries
    ] == [
        (urgent.task_id, 3, 0, urgent_at),
        (fifo_first.task_id, 4, 2, tied_at),
        (fifo_second.task_id, 5, 2, tied_at),
    ]
    assert [(entry.external_task_id, entry.status) for entry in overview.active_entries] == [
        (building.task_id, TaskStatus.BUILDING),
        (running.task_id, TaskStatus.IN_PROGRESS),
    ]
    payload = overview.model_dump(mode="json")
    assert payload["observed_at"] == now.isoformat()
    assert [entry["enqueued_at"] for entry in payload["waiting_entries"]] == [
        urgent_at.isoformat(),
        tied_at.isoformat(),
        tied_at.isoformat(),
    ]
    assert [entry["started_at"] for entry in payload["active_entries"]] == [
        (now - timedelta(minutes=30)).isoformat(),
        (now - timedelta(minutes=20)).isoformat(),
    ]
    assert overview.waiting_capped
    assert overview.active_capped


@pytest.mark.asyncio
async def test_overview_reports_empty_snapshot(database_session: Session) -> None:
    now = datetime(2026, 7, 17, 12, tzinfo=UTC)

    overview = await read_scheduler_overview(
        gate=cast(RedisQueueGate, FakeGate()),
        session=database_session,
        org_id=TEST_ORG_ID,
        now=now,
        waiting_limit=100,
        active_limit=100,
    )

    assert overview.summary.model_dump() == {"waiting": 0, "building": 0, "in_progress": 0, "evaluating": 0}
    assert not overview.pools
    assert not overview.waiting_entries
    assert not overview.active_entries
