"""Run-wide logs and persisted sandbox evidence."""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from benchmark_service.sandbox.daytona import DaytonaProviderConfig
from daytona import AsyncDaytona, DaytonaConfig, DaytonaError, DaytonaNotFoundError
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy.orm import defer
from sqlmodel import Session, col, select

from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import get_run_log_events, task_log_attempt_id
from tracker.aws.resolver import resolve_run_aws_runtime
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    Org,
    Task,
    TaskAttempt,
)
from tracker.database.scoping import get_scoped
from tracker.database.session import get_session
from tracker.exceptions import TrackerServiceError
from tracker.types import HarnessConfig, SandboxSnapshot
from tracker.utils.harness_config import try_fetch_harness_config
from tracker.utils.resources import fetch_sandbox_provider_config

router = APIRouter(prefix="/benchmarks")
AttemptId = Annotated[str, Path(min_length=1, max_length=32, pattern=r"^[0-9a-f]+$")]
_ACTIVE_RUN_STATUSES = {BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING}
_UNAVAILABLE_MESSAGE = "Live sandbox metadata is temporarily unavailable."
_UNSUPPORTED_MESSAGE = "Live sandbox metadata is not supported for this provider."
_NOT_RECORDED_MESSAGE = "No sandbox ID was recorded for this attempt."


class RunLogEventsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    limit: int = Field(default=500, ge=1, le=1_000)
    cursor: str | None = Field(default=None, min_length=1, max_length=4_096)


class RunLogEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    task_id: str
    attempt_id: str
    timestamp_ms: int
    ingestion_time_ms: int
    message: str


class RunLogEventsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    events: list[RunLogEvent]
    next_cursor: str
    at_tail: bool
    is_active: bool


class SandboxEvidenceBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str
    instance_id: str


class SandboxResources(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cpu_cores: float
    memory_gib: float
    disk_gib: float
    gpu_count: float
    gpu_type: str | None


class LiveSandboxEvidence(SandboxEvidenceBase):
    status: Literal["live"] = "live"
    name: str
    state: str | None
    region: str
    resources: SandboxResources
    created_at: str | None
    updated_at: str | None
    last_activity_at: str | None


class DeletedSandboxEvidence(SandboxEvidenceBase):
    status: Literal["deleted"] = "deleted"
    snapshot: SandboxSnapshot | None


class UnsupportedSandboxEvidence(SandboxEvidenceBase):
    status: Literal["unsupported"] = "unsupported"
    message: str = Field(max_length=200)
    snapshot: SandboxSnapshot | None


class UnavailableSandboxEvidence(SandboxEvidenceBase):
    status: Literal["unavailable"] = "unavailable"
    message: str = Field(max_length=200)
    snapshot: SandboxSnapshot | None


class NotRecordedSandboxEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["not_recorded"] = "not_recorded"
    provider: str
    instance_id: None = None
    message: str = Field(max_length=200)


SandboxEvidence = Annotated[
    LiveSandboxEvidence
    | DeletedSandboxEvidence
    | UnsupportedSandboxEvidence
    | UnavailableSandboxEvidence
    | NotRecordedSandboxEvidence,
    Field(discriminator="status"),
]


@router.get("/{benchmark_id}/logs/events", response_model=RunLogEventsResponse)
def get_benchmark_log_events(
    benchmark_id: UUID,
    request: Request,
    query: Annotated[RunLogEventsRequest, Query()],
    org: Org = Depends(get_current_org),
    legacy_harness_config: HarnessConfig | None = Depends(try_fetch_harness_config),
    session: Session = Depends(get_session),
) -> RunLogEventsResponse:
    """Page interleaved task logs for one run, oldest first."""
    benchmark = get_scoped(Benchmark, benchmark_id, session, org)
    runtime = resolve_run_aws_runtime(
        request,
        aws_managed=benchmark.aws_managed,
        tenant_id=org.name,
        legacy_harness_config=legacy_harness_config,
    ).runtime
    if not runtime.resources.log_group:
        raise HTTPException(status_code=404, detail="Run logs are unavailable")

    try:
        page = get_run_log_events(
            str(benchmark.id),
            runtime,
            limit=query.limit,
            cursor=query.cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return RunLogEventsResponse(
        events=[
            RunLogEvent(
                event_id=event.event_id,
                task_id=event.task_id,
                attempt_id=event.attempt_id,
                timestamp_ms=event.timestamp_ms,
                ingestion_time_ms=event.ingestion_time_ms,
                message=event.message,
            )
            for event in page.events
        ],
        next_cursor=page.next_cursor,
        at_tail=page.at_tail,
        is_active=benchmark.status in _ACTIVE_RUN_STATUSES,
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id:path}/attempts/{attempt_id}/sandbox",
    response_model=SandboxEvidence,
)
async def get_task_attempt_sandbox(
    benchmark_id: UUID,
    task_id: str,
    attempt_id: AttemptId,
    request: Request,
    org: Org = Depends(get_current_org),
    legacy_harness_config: HarnessConfig | None = Depends(try_fetch_harness_config),
    session: Session = Depends(get_session),
) -> SandboxEvidence:
    """Return persisted sandbox identity plus safe live provider metadata."""
    benchmark = get_scoped(Benchmark, benchmark_id, session, org)
    task = session.exec(
        select(Task)
        .where(col(Task.org_id) == org.id)
        .where(col(Task.benchmark) == benchmark.id)
        .where(col(Task.task_id) == task_id)
    ).first()
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    attempt = session.exec(
        select(TaskAttempt)
        .where(col(TaskAttempt.org_id) == org.id)
        .where(col(TaskAttempt.task) == task.id)
        .where(col(TaskAttempt.attempt_id) == attempt_id)
    ).first()
    evaluation = session.exec(
        select(EvaluationResult)
        .where(col(EvaluationResult.org_id) == org.id)
        .where(col(EvaluationResult.task) == task.id)
        .where(col(EvaluationResult.attempt_id) == attempt_id)
        .options(defer(EvaluationResult.result))  # pyright: ignore[reportArgumentType]
    ).first()
    error = (
        None
        if evaluation is not None
        else session.exec(
            select(ErrorResult)
            .where(col(ErrorResult.org_id) == org.id)
            .where(col(ErrorResult.task) == task.id)
            .where(col(ErrorResult.attempt_id) == attempt_id)
        ).first()
    )
    is_current = attempt_id == task_log_attempt_id(task.started_at)
    if attempt is None and evaluation is None and error is None and not is_current:
        raise HTTPException(status_code=404, detail="Attempt not found")

    provider = attempt.sandbox_provider if attempt is not None else benchmark.arguments.sandbox_provider
    instance_id = attempt.sandbox_instance_id if attempt is not None else None
    snapshot_data = attempt.sandbox_snapshot if attempt is not None else None
    if instance_id is None and evaluation is not None:
        instance_id = evaluation.instance_id
    if instance_id is None:
        return NotRecordedSandboxEvidence(
            provider=provider,
            message=_NOT_RECORDED_MESSAGE,
        )

    snapshot = SandboxSnapshot.model_validate(snapshot_data) if snapshot_data is not None else None
    if provider != "daytona":
        return UnsupportedSandboxEvidence(
            provider=provider,
            instance_id=instance_id,
            message=_UNSUPPORTED_MESSAGE,
            snapshot=snapshot,
        )

    secret_name = benchmark.arguments.sandbox_provider_secret_name
    if secret_name is None:
        return UnavailableSandboxEvidence(
            provider=provider,
            instance_id=instance_id,
            message=_UNAVAILABLE_MESSAGE,
            snapshot=snapshot,
        )

    runtime = resolve_run_aws_runtime(
        request,
        aws_managed=benchmark.aws_managed,
        tenant_id=org.name,
        legacy_harness_config=legacy_harness_config,
    ).runtime
    try:
        provider_config = fetch_sandbox_provider_config(secret_name, runtime.clients, provider)
        if not isinstance(provider_config, DaytonaProviderConfig):
            raise AssertionError("Daytona run resolved a non-Daytona provider config")
        async with AsyncDaytona(
            DaytonaConfig(
                api_key=provider_config.DAYTONA_API_KEY,
                api_url=provider_config.DAYTONA_API_URL,
                target=provider_config.DAYTONA_TARGET,
                otel_enabled=False,
            )
        ) as daytona:
            sandbox = await daytona.get(instance_id)
    except DaytonaNotFoundError:
        return DeletedSandboxEvidence(
            provider=provider,
            instance_id=instance_id,
            snapshot=snapshot,
        )
    except (DaytonaError, TrackerServiceError, ValidationError):
        return UnavailableSandboxEvidence(
            provider=provider,
            instance_id=instance_id,
            message=_UNAVAILABLE_MESSAGE,
            snapshot=snapshot,
        )

    return LiveSandboxEvidence(
        provider=provider,
        instance_id=instance_id,
        name=sandbox.name,
        state=sandbox.state.value if sandbox.state is not None else None,
        region=sandbox.target,
        resources=SandboxResources(
            cpu_cores=float(sandbox.cpu),
            memory_gib=float(sandbox.memory),
            disk_gib=float(sandbox.disk),
            gpu_count=float(sandbox.gpu),
            gpu_type=sandbox.gpu_type.value if sandbox.gpu_type is not None else None,
        ),
        created_at=sandbox.created_at,
        updated_at=sandbox.updated_at,
        last_activity_at=sandbox.last_activity_at,
    )
