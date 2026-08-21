"""Shared API dependencies."""

from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Request
from opentelemetry import trace
from sqlmodel import Session

from tracker.auth import get_current_org
from tracker.aws.resolver import resolve_agent_library_aws_runtime, resolve_run_aws_runtime
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import Benchmark, Org
from tracker.database.scoping import get_scoped
from tracker.database.session import get_session
from tracker.logging import benchmark_id_var


async def bind_benchmark_id(benchmark_id: UUID) -> UUID:
    """Bind a route's run identifier to logs, errors, and the active request span."""
    value = str(benchmark_id)
    benchmark_id_var.set(value)
    trace.get_current_span().set_attribute("benchmark_id", value)
    return benchmark_id


TrackedBenchmarkId = Annotated[UUID, Depends(bind_benchmark_id)]


def get_agent_library_aws_runtime(
    request: Request,
    org: Org = Depends(get_current_org),
) -> AWSRuntime:
    """Resolve AWS authority for agent-library operations."""
    return resolve_agent_library_aws_runtime(request, org.id)


@dataclass(frozen=True)
class RunAWSContext:
    """An organization-scoped run and its persisted AWS authority."""

    benchmark: Benchmark
    aws_runtime: AWSRuntime


def get_run_aws_context(
    benchmark_id: TrackedBenchmarkId,
    request: Request,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> RunAWSContext:
    """Return an organization-scoped run with its persisted AWS authority."""
    benchmark = get_scoped(Benchmark, benchmark_id, session, org)
    return RunAWSContext(
        benchmark=benchmark,
        aws_runtime=resolve_run_aws_runtime(
            request,
            aws_managed=benchmark.aws_managed,
            org_id=org.id,
        ),
    )


RunAWSDependency = Annotated[RunAWSContext, Depends(get_run_aws_context)]
