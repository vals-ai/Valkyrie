"""GET /costs/by-email and /costs/by-project — run costs for runs started in a window."""

from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, NamedTuple, Sequence, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func
from sqlmodel import Session, col, select

from tracker.auth import get_current_org
from tracker.database.models import Benchmark, Org, Project, RunCost, RunProject
from tracker.database.session import get_session

router = APIRouter(prefix="/costs")


class EmailCost(BaseModel):
    email: str
    cost_type: str
    provider: str
    amount_usd: float
    run_count: int


class CostsByEmailResponse(BaseModel):
    start: datetime
    end: datetime
    costs: list[EmailCost]


class _CostRow(NamedTuple):
    email: str
    cost_type: str
    provider: str
    amount_usd: float
    run_count: int


@router.get("/by-email", response_model=CostsByEmailResponse)
def get_costs_by_email(
    start: date = Query(description="First UTC date whose runs are counted."),
    end: date = Query(description="UTC date after the last counted one."),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> CostsByEmailResponse:
    """Each starter's recorded run costs in this org, per cost type and provider, for runs started in [start, end)."""
    if end <= start:
        raise HTTPException(status_code=422, detail="end must be after start")
    utc = ZoneInfo("UTC")
    window_start = datetime.combine(start, time(), tzinfo=utc)
    window_end = datetime.combine(end, time(), tzinfo=utc)
    email = func.lower(func.trim(col(Benchmark.started_by_email)))
    statement = (
        cast(Any, select)(
            email.label("email"),
            col(RunCost.cost_type).label("cost_type"),
            col(RunCost.provider).label("provider"),
            func.sum(col(RunCost.amount_usd)).label("amount_usd"),
            func.count(func.distinct(col(Benchmark.id))).label("run_count"),
        )
        .join(Benchmark, col(Benchmark.id) == col(RunCost.benchmark_id))
        .where(Benchmark.org_id == org.id)
        .where(col(Benchmark.started_by_email).is_not(None))
        .where(col(Benchmark.started_at) >= window_start)
        .where(col(Benchmark.started_at) < window_end)
        .group_by(email, col(RunCost.cost_type), col(RunCost.provider))
        .order_by(email, col(RunCost.cost_type), col(RunCost.provider))
    )
    rows = cast(Sequence[_CostRow], cast(Any, session.exec(statement)).all())
    return CostsByEmailResponse(
        start=window_start,
        end=window_end,
        costs=[EmailCost(**row._asdict()) for row in rows],
    )


class ProjectCostRow(BaseModel):
    month: str
    vendor: str
    spend_type: str | None
    subcategory: str | None
    project_handle: str | None
    project_name: str | None
    customer: str | None
    amount_usd: float
    run_count: int


class ProjectRun(BaseModel):
    project_handle: str | None
    run_id: UUID
    started_at: datetime


class CostsByProjectResponse(BaseModel):
    start: datetime
    end: datetime
    rows: list[ProjectCostRow]
    runs: list[ProjectRun]


class _RunProjectRow(NamedTuple):
    run_id: UUID
    started_at: datetime
    handle: str | None
    name: str | None
    spend_type: Any
    subcategory: str | None
    customer: str | None


class _RunProviderCost(NamedTuple):
    run_id: UUID
    provider: str
    amount_usd: float


def _as_utc(moment: datetime) -> datetime:
    utc = ZoneInfo("UTC")
    return moment.replace(tzinfo=utc) if moment.tzinfo is None else moment.astimezone(utc)


def _vendor(provider: str) -> str:
    return provider.title()


@router.get("/by-project", response_model=CostsByProjectResponse)
def get_costs_by_project(
    start: date = Query(description="First UTC date whose runs are counted."),
    end: date = Query(description="UTC date after the last counted one."),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> CostsByProjectResponse:
    """Recorded run costs per month, vendor and project, for runs started in [start, end).

    A run counts under the project of its newest link (the highest run_project id). Runs with no project come back
    with null project fields. `runs` lists every run in the window with its project so other cost sources can join.
    """
    if end <= start:
        raise HTTPException(status_code=422, detail="end must be after start")
    utc = ZoneInfo("UTC")
    window_start = datetime.combine(start, time(), tzinfo=utc)
    window_end = datetime.combine(end, time(), tzinfo=utc)
    ranked = (
        cast(Any, select)(
            col(RunProject.benchmark_id).label("benchmark_id"),
            col(RunProject.project_id).label("project_id"),
            func.row_number()
            .over(partition_by=col(RunProject.benchmark_id), order_by=col(RunProject.id).desc())
            .label("newest"),
        )
        .join(Benchmark, col(Benchmark.id) == col(RunProject.benchmark_id))
        .where(Benchmark.org_id == org.id)
        .where(col(Benchmark.started_at) >= window_start)
        .where(col(Benchmark.started_at) < window_end)
    ).subquery()
    current = cast(Any, select)(ranked.c.benchmark_id, ranked.c.project_id).where(ranked.c.newest == 1).subquery()
    runs_statement = (
        cast(Any, select)(
            col(Benchmark.id).label("run_id"),
            col(Benchmark.started_at).label("started_at"),
            col(Project.handle).label("handle"),
            col(Project.name).label("name"),
            col(Project.spend_type).label("spend_type"),
            col(Project.subcategory).label("subcategory"),
            col(Project.customer).label("customer"),
        )
        .select_from(Benchmark)
        .outerjoin(current, current.c.benchmark_id == col(Benchmark.id))
        .outerjoin(Project, (col(Project.id) == current.c.project_id) & (col(Project.org_id) == org.id))
        .where(Benchmark.org_id == org.id)
        .where(col(Benchmark.started_at) >= window_start)
        .where(col(Benchmark.started_at) < window_end)
        .order_by(col(Benchmark.started_at), col(Benchmark.id))
    )
    run_rows = cast(Sequence[_RunProjectRow], cast(Any, session.exec(runs_statement)).all())
    costs_statement = (
        cast(Any, select)(
            col(RunCost.benchmark_id).label("run_id"),
            col(RunCost.provider).label("provider"),
            func.sum(col(RunCost.amount_usd)).label("amount_usd"),
        )
        .join(Benchmark, col(Benchmark.id) == col(RunCost.benchmark_id))
        .where(Benchmark.org_id == org.id)
        .where(col(Benchmark.started_at) >= window_start)
        .where(col(Benchmark.started_at) < window_end)
        .group_by(col(RunCost.benchmark_id), col(RunCost.provider))
    )
    cost_rows = cast(Sequence[_RunProviderCost], cast(Any, session.exec(costs_statement)).all())
    by_run = {row.run_id: row for row in run_rows}
    grouped: dict[tuple[str, str, str | None], dict[str, Any]] = {}
    for cost in cost_rows:
        run = by_run[cost.run_id]
        month = _as_utc(run.started_at).strftime("%Y-%m")
        key = (month, _vendor(cost.provider), run.handle)
        group = grouped.setdefault(key, {"run": run, "amount_usd": 0.0, "run_ids": set()})
        group["amount_usd"] += cost.amount_usd
        group["run_ids"].add(cost.run_id)
    rows = [
        ProjectCostRow(
            month=month,
            vendor=vendor,
            spend_type=getattr(group["run"].spend_type, "value", group["run"].spend_type),
            subcategory=group["run"].subcategory,
            project_handle=handle,
            project_name=group["run"].name,
            customer=group["run"].customer,
            amount_usd=group["amount_usd"],
            run_count=len(group["run_ids"]),
        )
        for (month, vendor, handle), group in sorted(
            grouped.items(), key=lambda item: (item[0][0], item[0][1], item[0][2] or "")
        )
    ]
    return CostsByProjectResponse(
        start=window_start,
        end=window_end,
        rows=rows,
        runs=[
            ProjectRun(project_handle=row.handle, run_id=row.run_id, started_at=_as_utc(row.started_at))
            for row in run_rows
        ],
    )
