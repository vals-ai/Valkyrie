"""GET /benchmarks/status — lightweight polling for live status updates."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session, col, select

from tracker.auth import get_current_user_and_org
from tracker.database.models import Benchmark, Org, TaskStatus, User
from tracker.database.session import get_session
from tracker.types import BenchmarkStatusEntry, BenchmarkStatusResponse

router = APIRouter()


@router.get("/benchmarks/status", response_model=BenchmarkStatusResponse)
def get_benchmarks_status(
    ids: str = Query(default="", description="Comma-separated benchmark UUIDs"),
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> BenchmarkStatusResponse:
    _, org = user_and_org
    parsed_ids: list[UUID] = []
    for raw in ids.split(","):
        token = raw.strip()
        if not token:
            continue
        try:
            parsed_ids.append(UUID(token))
        except ValueError:
            continue

    if not parsed_ids:
        return BenchmarkStatusResponse(entries=[])

    benchmarks = session.exec(
        select(Benchmark).where(Benchmark.org_id == org.id).where(col(Benchmark.id).in_(parsed_ids))
    ).all()

    entries: list[BenchmarkStatusEntry] = []
    for b in benchmarks:
        state_counts = b.fetch_task_state_counts(session)
        total = sum(state_counts.values())
        finished = state_counts.get(TaskStatus.FINISHED, 0) + state_counts.get(TaskStatus.ERROR, 0)
        entries.append(
            BenchmarkStatusEntry(
                id=b.id,
                status=b.status,
                finished_at=b.finished_at,
                total_tasks=total,
                finished_tasks=finished,
                task_state_counts={k.value: v for k, v in state_counts.items()},
            )
        )

    return BenchmarkStatusResponse(entries=entries)
