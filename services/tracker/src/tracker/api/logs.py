"""GET /benchmarks/{id}/logs — CloudWatch event history for a run."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, select

from tracker.auth import get_current_user_and_org
from tracker.cloudwatch import filter_log_events
from tracker.database.models import Benchmark, Org, OrgConfig, User
from tracker.database.session import get_session
from tracker.types import AWSCredentials, LogEvent, LogsResponse

router = APIRouter()


def _load_benchmark_or_404(benchmark_id: UUID, org: Org, session: Session) -> Benchmark:
    bench = session.exec(
        select(Benchmark).where(Benchmark.id == benchmark_id).where(Benchmark.org_id == org.id)
    ).first()
    if bench is None:
        raise HTTPException(status_code=404, detail="Benchmark not found")
    return bench


@router.get("/benchmarks/{benchmark_id}/logs", response_model=LogsResponse)
def get_benchmark_logs(
    benchmark_id: UUID,
    limit: int = Query(default=200, ge=1, le=1000),
    next_token: str | None = None,
    start_time: int | None = Query(default=None, description="ms since epoch"),
    user_and_org: tuple[User | None, Org] = Depends(get_current_user_and_org),
    session: Session = Depends(get_session),
) -> LogsResponse:
    _, org = user_and_org
    bench = _load_benchmark_or_404(benchmark_id, org, session)

    config = session.exec(select(OrgConfig).where(OrgConfig.org_id == org.id)).first()
    if config is None or not config.log_group:
        return LogsResponse(events=[], next_token=None)

    aws = AWSCredentials(
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        aws_default_region=config.aws_default_region,
    )

    result = filter_log_events(
        benchmark_id=bench.id,
        aws=aws,
        log_group=config.log_group,
        limit=limit,
        next_token=next_token,
        start_time_ms=start_time,
    )

    return LogsResponse(
        events=[LogEvent(**e) for e in result["events"]],
        next_token=result["next_token"],
    )
