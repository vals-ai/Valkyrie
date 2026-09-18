"""Shared API dependencies."""

from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Request
from opentelemetry import trace
from sqlmodel import Session, select

from tracker.auth import get_current_org
from tracker.aws.resolver import (
    http_validate_saved_managed_storage_runtime,
    resolve_agent_library_aws_runtime,
    resolve_run_aws_runtime_and_access_key_config,
)
from tracker.aws.runtime import AWSRuntime
from tracker.aws.services import CloudRuntimeFactory
from tracker.database.models import Benchmark, Org, Task
from tracker.database.scoping import get_scoped
from tracker.database.session import get_session
from tracker.logging import benchmark_id_var
from tracker.local import config as local_config
from tracker.local.resources import LocalResources
from tracker.local.runtime import LocalRuntimeFactory
from tracker.runtime.services import RuntimeServices


async def bind_benchmark_id(benchmark_id: UUID) -> UUID:
    """Bind a route's run identifier to logs, errors, and the active request span."""
    value = str(benchmark_id)
    benchmark_id_var.set(value)
    trace.get_current_span().set_attribute("benchmark_id", value)
    return benchmark_id


TrackedBenchmarkId = Annotated[UUID, Depends(bind_benchmark_id)]


def get_run_benchmark(
    benchmark_id: TrackedBenchmarkId,
    session: Session = Depends(get_session),
    org: Org = Depends(get_current_org),
) -> Benchmark:
    """Load a run scoped to the authenticated organization."""
    return get_scoped(Benchmark, benchmark_id, session, org)


RunBenchmarkDependency = Annotated[Benchmark, Depends(get_run_benchmark)]


@dataclass(frozen=True)
class RunAWSContext:
    """An organization-scoped run and its persisted AWS authority."""

    benchmark: Benchmark
    aws_runtime: AWSRuntime


async def get_run_aws_context(
    benchmark: RunBenchmarkDependency,
    request: Request,
    org: Org = Depends(get_current_org),
) -> RunAWSContext:
    """Return an organization-scoped run with its persisted AWS authority."""
    assert benchmark.arguments.properties is None or not isinstance(benchmark.arguments.properties, LocalResources)
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


async def get_run_runtime(
    benchmark: RunBenchmarkDependency,
    request: Request,
    org: Org = Depends(get_current_org),
) -> RuntimeServices:
    """Compose services for one authorized run operation."""
    arguments = benchmark.arguments
    if arguments.environment == "local":
        assert isinstance(arguments.properties, LocalResources)
        return LocalRuntimeFactory.create_runtime(arguments.properties.data_root, org.id)

    run_context = await get_run_aws_context(benchmark, request, org)
    return CloudRuntimeFactory.create_runtime(
        run_context.aws_runtime,
        sandbox_provider=arguments.sandbox_provider,
        sandbox_provider_secret_name=arguments.sandbox_provider_secret_name,
    )


RunRuntimeDependency = Annotated[RuntimeServices, Depends(get_run_runtime)]


def get_agent_library_runtime(
    request: Request,
    org: Org = Depends(get_current_org),
) -> RuntimeServices:
    """Open agent storage without constructing sandbox access."""
    if local_config.resources is not None:
        return LocalRuntimeFactory.create_runtime(local_config.resources.data_root, org.id)
    aws_runtime = resolve_agent_library_aws_runtime(request, org.id)

    return CloudRuntimeFactory.create_runtime(aws_runtime)


AgentLibraryRuntimeDependency = Annotated[RuntimeServices, Depends(get_agent_library_runtime)]


def load_task_for_benchmark_or_404(benchmark: Benchmark, task_id: str, org: Org, session: Session) -> Task:
    """Return a task from an already organization-scoped benchmark."""
    task = session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.org_id == org.id).where(Task.task_id == task_id)
    ).first()
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task
