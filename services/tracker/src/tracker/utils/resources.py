"""Factory helpers that construct clients, provider configs, and validated DB rows."""

from dataclasses import dataclass
from uuid import UUID

from benchmark_service import SandboxProviderConfig
from benchmark_service.client import BenchmarkServiceClient
from sqlmodel import Session, select

from tracker.auth import RequestIdentity
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    Org,
    Task,
    benchmark_arguments_adapter,
)
from tracker.exceptions import TrackerServiceError
from tracker.outbound_security import validate_service_headers, validate_service_url_syntax
from tracker.runtime.secrets import SecretStore, sandbox_provider_config_from_secret
from tracker.types import (
    StartBenchmarkRequest,
)


@dataclass(frozen=True)
class BenchmarkConcurrencyUpdate:
    benchmark_id: UUID
    status: BenchmarkStatus
    concurrency: int


async def fetch_sandbox_provider_config(
    secret_name: str,
    secret_store: SecretStore,
    provider_type: str,
) -> SandboxProviderConfig:
    """Resolve sandbox provider config without blocking the caller's event loop."""
    return sandbox_provider_config_from_secret(await secret_store.get(secret_name), provider_type)


def create_benchmark_service_client(
    url: str,
    service_headers: dict[str, str] | None = None,
) -> BenchmarkServiceClient:
    """Create a BenchmarkServiceClient with benchmark-service headers."""
    url = validate_service_url_syntax(url)
    headers = dict(service_headers or {})
    validate_service_headers(headers)

    return BenchmarkServiceClient(url=url, headers=headers)


def start_benchmark_request_to_benchmark(
    request: StartBenchmarkRequest,
    run_starter: RequestIdentity,
    *,
    aws_managed: bool,
    queue_pool_id: str | None = None,
) -> Benchmark:
    """Convert a StartBenchmarkRequest to a Benchmark database model."""
    if request.environment == "aws" and aws_managed != (request.harness_config is None):
        raise ValueError("Benchmark AWS mode does not match the start request")
    provider_secret_name = request.sandbox_provider_secret_reference
    if aws_managed and (not request.sandbox_provider or not provider_secret_name):
        raise ValueError("Managed runs require a sandbox provider and provider secret name")

    return Benchmark(
        org_id=run_starter.org.id,
        name=request.benchmark_name,
        label=request.label,
        custom_benchmark_service=request.custom_benchmark_service,
        aws_managed=aws_managed,
        webhook_secret_name=request.webhook_secret_name,
        webhook_intervals=request.webhook_intervals,
        arguments=benchmark_arguments_adapter.validate_python(
            {
                "environment": request.environment,
                "properties": request.properties,
                "contract": request.contract,
                "concurrency": request.concurrency,
                "priority": request.priority,
                "queue_pool_id": queue_pool_id,
                "task_ids": request.task_ids,
                "slice_str": request.slice_str,
                "lambda_function": request.lambda_function,
                "dataset": request.dataset,
                "sandbox_provider": request.sandbox_provider,
                "sandbox_provider_secret_name": provider_secret_name,
            },
        ),
        started_by_id=run_starter.access_key_id,
        started_by_email=run_starter.email,
    )


def fetch_benchmark_row(
    benchmark_id: UUID,
    session: Session,
    org: Org,
    *,
    for_update: bool = False,
) -> Benchmark:
    """Fetch an org-scoped benchmark, optionally locking it for a state transition.

    Arguments
    - benchmark_id: Run identifier to fetch.
    - session: Database session used for the query.
    - org: Organization expected to contain the run.
    - for_update: Lock and refresh the row until the transaction completes.

    Returns
    - The matching benchmark row.

    Raises
    - ValueError: The run is missing or belongs to another organization.
    """
    benchmark_row = session.get(
        Benchmark,
        benchmark_id,
        populate_existing=for_update,
        with_for_update=for_update or None,
    )
    if not benchmark_row:
        raise ValueError(f"Run with id {benchmark_id} not found")
    if benchmark_row.org_id != org.id:
        raise ValueError(f"Run {benchmark_id} does not belong to org {org.id}")
    return benchmark_row


def _fetch_locked_benchmark(benchmark_id: UUID, session: Session, org: Org) -> Benchmark:
    benchmark_row = session.exec(
        select(Benchmark)
        .where(Benchmark.id == benchmark_id)
        .where(Benchmark.org_id == org.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    ).one_or_none()
    if benchmark_row is None:
        raise ValueError(f"Run with id {benchmark_id} not found")
    return benchmark_row


def update_benchmark_concurrency(
    benchmark_id: UUID,
    concurrency: int,
    session: Session,
    org: Org,
) -> BenchmarkConcurrencyUpdate:
    """Lock an org-scoped run and persist a new active-run concurrency limit."""
    benchmark_row = _fetch_locked_benchmark(benchmark_id, session, org)
    if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
        return BenchmarkConcurrencyUpdate(
            benchmark_id=benchmark_row.id,
            status=benchmark_row.status,
            concurrency=benchmark_row.arguments.concurrency,
        )

    benchmark_row.arguments = benchmark_row.arguments.model_copy(update={"concurrency": concurrency})
    result = BenchmarkConcurrencyUpdate(
        benchmark_id=benchmark_row.id,
        status=benchmark_row.status,
        concurrency=benchmark_row.arguments.concurrency,
    )
    session.commit()
    return result


def update_benchmark_resume_arguments(
    benchmark_id: UUID,
    session: Session,
    org: Org,
    *,
    secrets: dict[str, str],
    concurrency: int | None,
    benchmark_url: str | None,
) -> Benchmark:
    """Lock and persist argument and service URL overrides used by resume and retry."""
    benchmark_row = _fetch_locked_benchmark(benchmark_id, session, org)
    arguments = benchmark_row.arguments

    if secrets:
        contract = arguments.contract
        updated_contract = contract.model_copy(update={"secrets": {**contract.secrets, **secrets}})
        arguments = arguments.model_copy(update={"contract": updated_contract})
    if concurrency is not None:
        arguments = arguments.model_copy(update={"concurrency": concurrency})
    if benchmark_url is not None:
        benchmark_row.custom_benchmark_service = benchmark_url

    benchmark_row.arguments = arguments
    session.add(benchmark_row)
    return benchmark_row


def fetch_task_row(task_id: UUID, session: Session, org: Org) -> Task:
    """Fetch task row with org validation. Raises domain errors (not HTTPException) for use in background tasks."""
    task_row = session.get(Task, task_id)
    if not task_row:
        raise TrackerServiceError(f"Task with id {task_id} not found")
    if task_row.org_id != org.id:
        raise TrackerServiceError(f"Task {task_id} does not belong to org {org.id}")
    return task_row
