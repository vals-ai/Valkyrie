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


@router.get("/filter-options", response_model=FilterOptionsResponse)
def get_filter_options(
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> FilterOptionsResponse:
    """Distinct benchmark + agent names in this org, for the runs-list filter dropdowns."""

    benchmark_names = sorted(
        set(session.exec(select(Benchmark.name).where(Benchmark.org_id == org.id).distinct()).all())
    )

    rows = session.exec(select(Benchmark.arguments).where(Benchmark.org_id == org.id)).all()
    agent_names = sorted({row.contract.name for row in rows if row and row.contract.name})

    return FilterOptionsResponse(
        benchmark_names=benchmark_names,
        agent_names=agent_names,
    )
