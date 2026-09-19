"""GET /benchmarks/filter-options — distinct values for filter dropdowns."""

from __future__ import annotations

from typing import Any, Sequence, cast

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


@router.get("/filter-options", response_model=FilterOptionsResponse)
def get_filter_options(
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> FilterOptionsResponse:
    """Distinct benchmark, agent, model, dataset, and starter values in this org, for the runs-list filter dropdowns."""

    arguments_json = type_coerce(col(Benchmark.arguments), JSON)
    statement = cast(Any, select)(
        col(Benchmark.name),
        arguments_json["contract"]["name"].as_string(),
        arguments_json["contract"]["model"].as_string(),
        arguments_json["dataset"].as_string(),
        col(Benchmark.started_by_email),
    )
    statement = statement.where(Benchmark.org_id == org.id).distinct()
    rows = cast(
        Sequence[tuple[str, str, str | None, str | None, str | None]],
        cast(Any, session.exec(statement)).all(),
    )
    return FilterOptionsResponse(
        benchmark_names=sorted({name for name, _, _, _, _ in rows}),
        agent_names=sorted({agent_name for _, agent_name, _, _, _ in rows}),
        models=sorted({model for _, _, model, _, _ in rows if model}),
        datasets=sorted({dataset or "default" for _, _, _, dataset, _ in rows}),
        started_by_emails=sorted({email for _, _, _, _, email in rows if email}),
    )
