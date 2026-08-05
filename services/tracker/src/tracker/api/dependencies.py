"""Shared API dependencies."""

from typing import Annotated
from uuid import UUID

from fastapi import Depends, Request
from sqlmodel import Session

from tracker.auth import get_current_org
from tracker.aws.resolver import resolve_run_aws_runtime
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import Benchmark, Org
from tracker.database.scoping import get_scoped
from tracker.database.session import get_session


def get_run_with_aws(
    benchmark_id: UUID,
    request: Request,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> tuple[Benchmark, AWSRuntime]:
    """Return an organization-scoped run with its persisted AWS authority."""
    benchmark = get_scoped(Benchmark, benchmark_id, session, org)
    runtime = resolve_run_aws_runtime(
        request,
        aws_managed=benchmark.aws_managed,
        org_id=org.id,
    )
    return benchmark, runtime


RunWithAWS = Annotated[tuple[Benchmark, AWSRuntime], Depends(get_run_with_aws)]
