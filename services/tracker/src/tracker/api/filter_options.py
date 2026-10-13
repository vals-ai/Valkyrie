"""GET /benchmarks/filter-options — distinct values for filter dropdowns."""

from __future__ import annotations

from typing import Any, NamedTuple, Sequence, cast

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import JSON, type_coerce
from sqlmodel import Session, col, select

from tracker.auth import get_current_org
from tracker.database.models import Benchmark, Org
from tracker.database.session import get_session

router = APIRouter(prefix="/benchmarks")


class FilterOptionsResponse(BaseModel):
    benchmark_names: list[str]
    agent_names: list[str]
    models: list[str]
    datasets: list[str]
    started_by_emails: list[str]


class _FilterRow(NamedTuple):
    benchmark_name: str
    agent_name: str
    model: str | None
    dataset: str | None
    email: str | None


@router.get("/filter-options", response_model=FilterOptionsResponse)
def get_filter_options(
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> FilterOptionsResponse:
    """Distinct benchmark, agent, model, dataset, and starter values in this org, for the runs-list filter dropdowns."""

    arguments = type_coerce(col(Benchmark.arguments), JSON)
    statement = (
        cast(Any, select)(
            col(Benchmark.name).label("benchmark_name"),
            arguments["contract"]["name"].as_string().label("agent_name"),
            arguments["contract"]["model"].as_string().label("model"),
            arguments["dataset"].as_string().label("dataset"),
            col(Benchmark.started_by_email).label("email"),
        )
        .where(Benchmark.org_id == org.id)
        .distinct()
    )
    rows = cast(Sequence[_FilterRow], cast(Any, session.exec(statement)).all())
    return FilterOptionsResponse(
        benchmark_names=sorted({row.benchmark_name for row in rows}),
        agent_names=sorted({row.agent_name for row in rows}),
        models=sorted({row.model for row in rows if row.model}),
        datasets=sorted({row.dataset or "default" for row in rows}),
        started_by_emails=sorted({row.email for row in rows if row.email}),
    )
