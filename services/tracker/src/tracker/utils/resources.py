"""Factory helpers that construct clients, provider configs, and validated DB rows."""

from dataclasses import dataclass
from uuid import UUID

from benchmark_service import (
    SandboxProviderConfig,
    sandbox_provider_config_from_mapping,
)
from benchmark_service.client import BenchmarkServiceClient
from sqlmodel import Session
from tracker.auth import RequestIdentity
from tracker.aws.secrets import fetch_aws_secret
from tracker.config import create_benchmark_service_url
from tracker.database.models import Benchmark, BenchmarkArguments, BenchmarkStatus, Org, Task
from tracker.database.repositories import BenchmarkRepository, TaskRepository
from tracker.exceptions import TrackerServiceError
from tracker.outbound_security import validate_service_headers, validate_service_url_syntax
from tracker.types import (
    AWSCredentials,
    StartBenchmarkRequest,
)


@dataclass(frozen=True)
class BenchmarkConcurrencyUpdate:
    benchmark_id: UUID
    status: BenchmarkStatus
    concurrency: int


def fetch_sandbox_provider_config(
    secret_name: str,
    aws: AWSCredentials | None,
    provider_type: str,
) -> SandboxProviderConfig:
    """Resolve sandbox provider config from the selected provider type and secret."""
    secret = fetch_aws_secret(secret_name, aws)
    if not isinstance(secret, dict):
        raise TrackerServiceError("Expected sandbox provider secret to be a JSON object")

    return sandbox_provider_config_from_mapping({**secret, "type": provider_type})


def create_benchmark_service_client(
    url: str,
    service_headers: dict[str, str] | None = None,
) -> BenchmarkServiceClient:
    """Create a BenchmarkServiceClient with benchmark-service headers."""
    url = validate_service_url_syntax(url)
    headers = dict(service_headers or {})
    validate_service_headers(headers)

    return BenchmarkServiceClient(url=url, headers=headers)


def create_benchmark_service_client_from_request(request: StartBenchmarkRequest) -> BenchmarkServiceClient:
    """Create a BenchmarkServiceClient for a start request."""
    url = request.custom_benchmark_service or create_benchmark_service_url(request.benchmark_name)
    return create_benchmark_service_client(url, service_headers=request.service_headers)


def start_benchmark_request_to_benchmark(request: StartBenchmarkRequest, run_starter: RequestIdentity) -> Benchmark:
    """Convert a StartBenchmarkRequest to a Benchmark database model."""
    return Benchmark(
        org_id=run_starter.org.id,
        name=request.benchmark_name,
        label=request.label,
        custom_benchmark_service=request.custom_benchmark_service,
        webhook_secret_name=request.webhook_secret_name,
        webhook_intervals=request.webhook_intervals,
        arguments=BenchmarkArguments(
            contract=request.contract,
            concurrency=request.concurrency,
            task_ids=request.task_ids,
            slice_str=request.slice_str,
            lambda_function=request.lambda_function,
            dataset=request.dataset,
            sandbox_provider=request.sandbox_provider,
            sandbox_provider_secret_name=request.harness_config.sandbox_provider_secret_name,
        ),
        started_by_id=run_starter.access_key_id,
        started_by_email=run_starter.email,
    )


def fetch_benchmark_row(
    benchmark_id: UUID,
    benchmark_repository: BenchmarkRepository,
    org: Org,
    *,
    for_update: bool = False,
) -> Benchmark:
    """Fetch an org-scoped benchmark, optionally locking it for a state transition.

    Arguments
    - benchmark_id: Run identifier to fetch.
    - benchmark_repository: Repository bound to the caller's database session.
    - org: Organization expected to contain the run.
    - for_update: Lock and refresh the row until the transaction completes.

    Returns
    - The matching benchmark row.

    Raises
    - ValueError: The run is missing or belongs to another organization.
    """
    benchmark_row = benchmark_repository.get_for_org(benchmark_id, org.id, for_update=for_update)
    if benchmark_row:
        return benchmark_row

    if benchmark_repository.get_by_id(benchmark_id) is not None:
        raise ValueError(f"Run {benchmark_id} does not belong to org {org.id}")

    raise ValueError(f"Run with id {benchmark_id} not found")


def update_benchmark_concurrency(
    benchmark_id: UUID,
    concurrency: int,
    session: Session,
    benchmark_repository: BenchmarkRepository,
    org: Org,
) -> BenchmarkConcurrencyUpdate:
    """Lock an org-scoped run and stage a new active-run concurrency limit for the caller to commit."""
    benchmark_row = fetch_benchmark_row(benchmark_id, benchmark_repository, org, for_update=True)
    if benchmark_row.status != BenchmarkStatus.IN_PROGRESS:
        return BenchmarkConcurrencyUpdate(
            benchmark_id=benchmark_row.id,
            status=benchmark_row.status,
            concurrency=benchmark_row.arguments.concurrency,
        )

    benchmark_repository.stage_concurrency(benchmark_row, concurrency)
    return BenchmarkConcurrencyUpdate(
        benchmark_id=benchmark_row.id,
        status=benchmark_row.status,
        concurrency=benchmark_row.arguments.concurrency,
    )


def update_benchmark_resume_arguments(
    benchmark_id: UUID,
    benchmark_repository: BenchmarkRepository,
    session: Session,
    org: Org,
    *,
    secrets: dict[str, str],
    concurrency: int | None,
    benchmark_url: str | None,
) -> Benchmark:
    """Lock and persist argument and service URL overrides used by resume and retry."""
    benchmark_row = fetch_benchmark_row(benchmark_id, benchmark_repository, org, for_update=True)
    arguments = benchmark_row.arguments

    if secrets:
        contract = arguments.contract
        updated_contract = contract.model_copy(update={"secrets": {**contract.secrets, **secrets}})
        arguments = arguments.model_copy(update={"contract": updated_contract})
    if concurrency is not None:
        arguments = arguments.model_copy(update={"concurrency": concurrency})
    benchmark_repository.stage_resume_arguments(benchmark_row, arguments, benchmark_url)
    return benchmark_row


def fetch_task_row(task_id: UUID, task_repository: TaskRepository, org: Org) -> Task:
    """Fetch task row with org validation. Raises domain errors (not HTTPException) for use in background tasks."""
    task_row = task_repository.get_by_id_for_org(task_id, org.id)
    if task_row:
        return task_row

    if task_repository.get_by_id(task_id) is not None:
        raise TrackerServiceError(f"Task {task_id} does not belong to org {org.id}")

    raise TrackerServiceError(f"Task with id {task_id} not found")
