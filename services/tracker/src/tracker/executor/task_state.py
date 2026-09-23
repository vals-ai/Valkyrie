"""Task-row initialization shared by legacy executors and Tracker's executor API."""

from collections.abc import Sequence
from datetime import datetime

from sqlmodel import Session, col, select

from tracker.database.models import Benchmark, Org, Task, TaskStatus


def ensure_task_rows(
    session: Session, benchmark: Benchmark, org: Org, task_ids: Sequence[str], *, started_at: datetime | None = None
) -> None:
    """Create missing rows while the caller holds the benchmark's authority lock."""
    existing_task_ids = set(
        session.exec(
            select(Task.task_id).where(Task.benchmark == benchmark.id).where(col(Task.task_id).in_(task_ids))
        ).all()
    )
    for task_id in task_ids:
        if task_id not in existing_task_ids:
            task = Task(org_id=org.id, task_id=task_id, benchmark=benchmark.id)
            if started_at is not None:
                task.started_at = started_at
            session.add(task)
            existing_task_ids.add(task_id)
    session.flush()


def load_task_rows(
    session: Session,
    benchmark: Benchmark,
    org: Org,
    task_ids: Sequence[str],
    *,
    statuses: Sequence[TaskStatus] | None = None,
) -> Sequence[tuple[str, Task]]:
    query = (
        select(Task)
        .where(Task.benchmark == benchmark.id)
        .where(Task.org_id == org.id)
        .where(col(Task.task_id).in_(task_ids))
        .execution_options(populate_existing=True)
    )
    if statuses is not None:
        query = query.where(col(Task.status).in_(statuses))
    rows_by_id = {task.task_id: task for task in session.exec(query).all()}

    return [(task_id, rows_by_id[task_id]) for task_id in task_ids if task_id in rows_by_id]
