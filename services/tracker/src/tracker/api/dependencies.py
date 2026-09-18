"""Shared API dependencies."""

from asyncio import to_thread
from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Request
from opentelemetry import trace
from sqlmodel import Session, select

from tracker.auth import get_current_org
from tracker.aws.historical_logs import historical_log_reader
from tracker.aws.resolver import (
    http_validate_saved_managed_storage_runtime,
    resolve_agent_library_aws_runtime,
    resolve_run_aws_runtime_and_access_key_config,
)
from tracker.aws.runtime import AWSRuntime
from tracker.aws.services import CloudRuntimeFactory
from tracker.database.models import Benchmark, BenchmarkStatus, Org, Task
from tracker.database.scoping import get_scoped
from tracker.database.session import get_session
from tracker.logging import benchmark_id_var
from tracker.runtime.logs import LogProviderError
from tracker.runtime.services import RuntimeServices


async def bind_benchmark_id(benchmark_id: UUID) -> UUID:
    """Bind a route's run identifier to logs, errors, and the active request span."""
    value = str(benchmark_id)
    benchmark_id_var.set(value)
    trace.get_current_span().set_attribute("benchmark_id", value)
    return benchmark_id


TrackedBenchmarkId = Annotated[UUID, Depends(bind_benchmark_id)]


@dataclass(frozen=True)
class RunAWSContext:
    """An organization-scoped run and its persisted AWS authority."""

    benchmark: Benchmark
    aws_runtime: AWSRuntime


async def get_run_aws_context(
    benchmark_id: TrackedBenchmarkId,
    request: Request,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> RunAWSContext:
    """Return an organization-scoped run with its persisted AWS authority."""
    benchmark = get_scoped(Benchmark, benchmark_id, session, org)
    aws_runtime = resolve_run_aws_runtime_and_access_key_config(
        request,
        aws_managed=benchmark.aws_managed,
        properties=benchmark.arguments.properties,
        org_id=org.id,
    ).runtime
    if benchmark.aws_managed:
        await http_validate_saved_managed_storage_runtime(aws_runtime, org_id=org.id)

    return RunAWSContext(
        benchmark=benchmark,
        aws_runtime=aws_runtime,
    )


RunAWSDependency = Annotated[RunAWSContext, Depends(get_run_aws_context)]


async def get_run_runtime(run_context: RunAWSDependency) -> RuntimeServices:
    """Compose services for one authorized run operation."""
    aws_runtime = run_context.aws_runtime
    arguments = run_context.benchmark.arguments

    runtime = CloudRuntimeFactory.create_runtime(
        aws_runtime,
        sandbox_provider=arguments.sandbox_provider,
        sandbox_provider_secret_name=arguments.sandbox_provider_secret_name,
    )
    benchmark = run_context.benchmark
    if benchmark.log_history is not None:
        if benchmark.arguments.properties is None:
            raise HTTPException(status_code=502, detail="Historical archive requires saved runtime resources")

        try:
            runtime.log_reader = await to_thread(
                historical_log_reader,
                aws_runtime,
                benchmark.id,
                benchmark.org_id,
                benchmark.log_history,
                runtime.log_reader,
                terminal=benchmark.status in {BenchmarkStatus.FINISHED, BenchmarkStatus.ERROR, BenchmarkStatus.STOPPED},
            )
        except LogProviderError as error:
            raise HTTPException(status_code=502, detail=str(error)) from error

    return runtime


RunRuntimeDependency = Annotated[RuntimeServices, Depends(get_run_runtime)]


async def get_agent_library_runtime(
    request: Request,
    org: Org = Depends(get_current_org),
) -> RuntimeServices:
    """Open agent storage without constructing sandbox access."""
    aws_runtime = resolve_agent_library_aws_runtime(request, org.id)

    runtime = CloudRuntimeFactory.create_runtime(aws_runtime)
    return runtime


AgentLibraryRuntimeDependency = Annotated[RuntimeServices, Depends(get_agent_library_runtime)]


def load_task_for_benchmark_or_404(benchmark: Benchmark, task_id: str, org: Org, session: Session) -> Task:
    """Return a task from an already organization-scoped benchmark."""
    task = session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.org_id == org.id).where(Task.task_id == task_id)
    ).first()
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task
