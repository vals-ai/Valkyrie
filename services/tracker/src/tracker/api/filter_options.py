"""GET /benchmarks/filter-options — distinct values for filter dropdowns."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlmodel import Session, select

from tracker.auth import get_current_user_and_org
from tracker.database.models import Benchmark, Org, User
from tracker.database.session import get_session

router = APIRouter()


class FilterOptionsResponse(BaseModel):
    benchmark_names: list[str]
    agent_names: list[str]


@router.get("/benchmarks/filter-options", response_model=FilterOptionsResponse)
def get_filter_options(
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> FilterOptionsResponse:
    _, org = user_and_org

    benchmark_names = sorted(
        set(
            session.exec(
                select(Benchmark.name)
                .where(Benchmark.org_id == org.id)
                .distinct()
            ).all()
        )
    )

    # Agent name lives in arguments.contract.name — extract via JSON path.
    # We iterate Python-side since SQLite (test) and Postgres differ on JSON path syntax.
    rows = session.exec(
        select(Benchmark.arguments).where(Benchmark.org_id == org.id)
    ).all()
    agent_names = sorted({r.contract.name for r in rows if r and r.contract.name})

    return FilterOptionsResponse(
        benchmark_names=benchmark_names,
        agent_names=agent_names,
    )
