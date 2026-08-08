"""GET /benchmarks/filter-options — distinct values for filter dropdowns."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from tracker.auth import get_current_org
from tracker.database.dependencies import BenchmarkRepositoryDep
from tracker.database.models import Org

router = APIRouter(prefix="/benchmarks")


class FilterOptionsResponse(BaseModel):
    benchmark_names: list[str]
    agent_names: list[str]


@router.get("/filter-options", response_model=FilterOptionsResponse)
def get_filter_options(
    benchmark_repository: BenchmarkRepositoryDep,
    org: Org = Depends(get_current_org),
) -> FilterOptionsResponse:
    """Distinct benchmark + agent names in this org, for the runs-list filter dropdowns."""

    benchmark_names, agent_names = benchmark_repository.get_filter_options(org.id)

    return FilterOptionsResponse(
        benchmark_names=benchmark_names,
        agent_names=agent_names,
    )
