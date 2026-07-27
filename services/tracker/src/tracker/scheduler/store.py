"""PostgreSQL-backed sandbox queue policy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import cast
from uuid import UUID

from sqlalchemy import JSON, func, type_coerce
from sqlalchemy.orm import aliased
from sqlmodel import Session, col, select, update

from tracker.database.models import Benchmark, BenchmarkStatus, Task, TaskStatus

_ACTIVE_TASK_STATUSES = (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)


@dataclass(frozen=True, slots=True)
class ScheduledTask:
    task_row_id: UUID
    task_id: str
    benchmark_id: UUID
    started_at: datetime
    priority: int


def queue_pool_id(provider_pool_id: str) -> str:
    """Return the stable non-secret identifier persisted for a provider pool."""
    return f"pool_{sha256(provider_pool_id.encode()).hexdigest()[:24]}"


def next_eligible_task(session: Session, pool_id: str) -> ScheduledTask | None:
    """Select the global queue head whose run has current concurrency capacity."""
    active_task = aliased(Task)
    active_count = (
        select(func.count(col(active_task.id)))
        .where(col(active_task.benchmark) == col(Benchmark.id))
        .where(col(active_task.status).in_(_ACTIVE_TASK_STATUSES))
        .correlate(Benchmark)
        .scalar_subquery()
    )
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    priority = arguments["priority"].as_integer()
    concurrency = arguments["concurrency"].as_integer()
    queued_pool = arguments["queue_pool_id"].as_string()

    row = cast(
        tuple[Task, int] | None,
        session.exec(
            select(Task, priority.label("priority"))
            .join(Benchmark, col(Benchmark.id) == col(Task.benchmark))
            .where(col(Task.status) == TaskStatus.PENDING)
            .where(col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS)
            .where(queued_pool == pool_id)
            .where(priority.between(0, 4))
            .where(active_count < concurrency)
            .order_by(priority, col(Task.started_at), col(Task.id))
            .limit(1)
        ).first(),
    )
    if row is None:
        return None

    task, selected_priority = row

    return ScheduledTask(
        task_row_id=task.id,
        task_id=task.task_id,
        benchmark_id=task.benchmark,
        started_at=task.started_at,
        priority=selected_priority,
    )


def reset_abandoned_builds(session: Session, pool_id: str, now: datetime) -> int:
    """Return abandoned sandbox builds in one provider pool to the queue."""
    arguments = type_coerce(col(Benchmark.arguments), JSON)
    queued_benchmarks = select(col(Benchmark.id)).where(
        col(Benchmark.status) == BenchmarkStatus.IN_PROGRESS,
        arguments["queue_pool_id"].as_string() == pool_id,
    )
    result = session.exec(
        update(Task)
        .where(col(Task.status) == TaskStatus.BUILDING)
        .where(col(Task.benchmark).in_(queued_benchmarks))
        .values(status=TaskStatus.PENDING, started_at=now)
    )

    return result.rowcount
