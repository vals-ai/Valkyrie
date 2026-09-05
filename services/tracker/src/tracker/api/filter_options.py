"""GET /benchmarks/filter-options — distinct values for filter dropdowns."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import Session, select

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

    benchmarks = session.exec(select(Benchmark).where(Benchmark.org_id == org.id)).all()
    return FilterOptionsResponse(
        benchmark_names=sorted({b.name for b in benchmarks}),
        agent_names=sorted({b.arguments.contract.name for b in benchmarks}),
        models=sorted({b.arguments.contract.model for b in benchmarks if b.arguments.contract.model}),
        datasets=sorted({b.arguments.dataset or "default" for b in benchmarks}),
        started_by_emails=sorted({b.started_by_email for b in benchmarks if b.started_by_email}),
    )
