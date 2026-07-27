"""Tests for PostgreSQL scheduler selection.

Run: uv run pytest tests/unit/scheduler/test_store.py

Covers provider-pool hashing and global priority/FIFO selection.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlmodel import Session

from tests.factories import make_task
from tests.utils import TEST_ORG_ID
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkArguments, Task, TaskStatus
from tracker.scheduler.store import next_eligible_task, queue_pool_id

_NOW = datetime(2026, 7, 27, 12, tzinfo=UTC)


def _make_benchmark(
    session: Session,
    *,
    org_id: UUID,
    name: str,
    pool_id: str,
    priority: int,
    concurrency: int = 1,
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


class TestQueuePoolId:
    """Stable provider-pool identifiers."""

    def test_hashes_provider_pool_deterministically(self) -> None:
        assert queue_pool_id("daytona:organization") == "pool_37e739bd97d514b9ed9df416"
        assert queue_pool_id("daytona:other") != queue_pool_id("daytona:organization")


class TestNextEligibleTask:
    """Global task selection across queued runs."""

    def test_orders_priority_then_fifo_and_skips_saturated_run(
        self,
        database_session: Session,
    ) -> None:
        pool_id = queue_pool_id("daytona:organization")
        blocked_run = _make_benchmark(
            database_session,
            org_id=TEST_ORG_ID,
            name="blocked-priority-zero",
            pool_id=pool_id,
            priority=0,
            concurrency=3,
        )
        fifo_run = _make_benchmark(
            database_session,
            org_id=TEST_ORG_ID,
            name="priority-one",
            pool_id=pool_id,
            priority=1,
            concurrency=2,
        )
        priority_two_run = _make_benchmark(
            database_session,
            org_id=TEST_ORG_ID,
            name="priority-two",
            pool_id=pool_id,
            priority=2,
        )
        priority_three_run = _make_benchmark(
            database_session,
            org_id=TEST_ORG_ID,
            name="priority-three",
            pool_id=pool_id,
            priority=3,
        )
        lower_priority_run = _make_benchmark(
            database_session,
            org_id=TEST_ORG_ID,
            name="priority-four",
            pool_id=pool_id,
            priority=4,
        )
        other_pool_run = _make_benchmark(
            database_session,
            org_id=TEST_ORG_ID,
            name="other-pool",
            pool_id=queue_pool_id("daytona:other"),
            priority=0,
        )

        database_session.add_all(
            [
                make_task(blocked_run, "blocked-active", status=TaskStatus.BUILDING, started_at=_NOW),
                make_task(
                    blocked_run,
                    "blocked-running",
                    status=TaskStatus.IN_PROGRESS,
                    started_at=_NOW,
                ),
                make_task(
                    blocked_run,
                    "blocked-evaluating",
                    status=TaskStatus.EVALUATING,
                    started_at=_NOW,
                ),
                make_task(
                    blocked_run,
                    "blocked-pending",
                    started_at=_NOW - timedelta(minutes=10),
                ),
                make_task(
                    fifo_run,
                    "fifo-newer",
                    started_at=_NOW - timedelta(minutes=1),
                ),
                make_task(
                    fifo_run,
                    "fifo-older",
                    started_at=_NOW - timedelta(minutes=2),
                ),
                make_task(
                    priority_two_run,
                    "priority-two",
                    started_at=_NOW - timedelta(hours=3),
                ),
                make_task(
                    priority_three_run,
                    "priority-three",
                    started_at=_NOW - timedelta(hours=2),
                ),
                make_task(
                    lower_priority_run,
                    "lower-priority-older",
                    started_at=_NOW - timedelta(hours=1),
                ),
                make_task(
                    other_pool_run,
                    "other-pool-priority-zero",
                    started_at=_NOW - timedelta(days=1),
                ),
            ]
        )
        database_session.commit()

        scheduled = next_eligible_task(database_session, pool_id)

        assert scheduled is not None
        assert scheduled.task_id == "fifo-older"
        assert scheduled.benchmark_id == fifo_run.id
        assert scheduled.priority == 1

        first_task = database_session.get(Task, scheduled.task_row_id)
        assert first_task is not None
        first_task.status = TaskStatus.BUILDING
        database_session.add(first_task)
        database_session.commit()

        next_in_fifo = next_eligible_task(database_session, pool_id)

        assert next_in_fifo is not None
        assert next_in_fifo.task_id == "fifo-newer"

        fifo_run.arguments = fifo_run.arguments.model_copy(update={"concurrency": 1})
        database_session.add(fifo_run)
        database_session.commit()

        after_concurrency_decrease = next_eligible_task(database_session, pool_id)

        assert after_concurrency_decrease is not None
        assert after_concurrency_decrease.task_id == "priority-two"
        assert after_concurrency_decrease.priority == 2
