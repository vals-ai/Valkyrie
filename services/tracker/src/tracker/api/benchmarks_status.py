"""GET /benchmarks/status — lightweight polling for live status updates."""
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session, col, func, select

from tracker.auth import get_current_user_and_org
from tracker.database.models import Benchmark, Org, Task, TaskStatus, User
from tracker.database.session import get_session
from tracker.types import BenchmarkStatusEntry, BenchmarkStatusResponse

router = APIRouter()


@router.get("/benchmarks/status", response_model=BenchmarkStatusResponse)
def get_benchmarks_status(
    ids: list[UUID] = Query(default_factory=list),
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> BenchmarkStatusResponse:
    _, org = user_and_org
    if not ids:
        return BenchmarkStatusResponse(entries=[])

    benchmarks = session.exec(
        select(Benchmark)
        .where(Benchmark.org_id == org.id)
        .where(col(Benchmark.id).in_(ids))
    ).all()

    entries: list[BenchmarkStatusEntry] = []
    for b in benchmarks:
        total = session.exec(
            select(func.count(col(Task.task_id)))
            .where(col(Task.benchmark) == b.id)
            .where(col(Task.org_id) == b.org_id)
        ).one()
        finished = session.exec(
            select(func.count(col(Task.task_id)))
            .where(col(Task.benchmark) == b.id)
            .where(col(Task.org_id) == b.org_id)
            .where(col(Task.status).in_([TaskStatus.FINISHED, TaskStatus.ERROR]))
        ).one()
        entries.append(
            BenchmarkStatusEntry(
                id=b.id,
                status=b.status,
                finished_at=b.finished_at,
                total_tasks=total,
                finished_tasks=finished,
            )
        )

    return BenchmarkStatusResponse(entries=entries)
