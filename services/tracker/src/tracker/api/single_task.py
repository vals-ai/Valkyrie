"""Per-task drill-in endpoints."""

from __future__ import annotations

from typing import cast
from uuid import UUID

from benchmark_service.sandbox.types import SandboxNotFoundError
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlmodel import Session, col, desc, select

from tracker.api.dependencies import (
    RunBenchmarkDependency,
    RunRuntimeDependency,
    TrackedBenchmarkId,
    load_task_for_benchmark_or_404,
)
from tracker.api.download import resolve_download_url
from tracker.auth import get_current_org
from tracker.runtime.logs import task_log_stream_name
from tracker.aws.s3 import S3_BENCHMARKS_PREFIX
from tracker.aws.services import CloudRuntimeServices
from tracker.database.models import (
    ActiveNativeOutput,
    Benchmark,
    ErrorResult,
    EvaluationResult,
    FailureCategory,
    Org,
    Task,
    TaskStatus,
    TaskBreakdown as TaskBreakdownRow,
)
from tracker.database.session import get_session
from tracker.types import (
    NativeOutputFile,
    NativeOutputFilesResponse,
    SingleTaskResponse,
    TaskArtifactsResponse,
    TaskBreakdown as TaskBreakdownResponse,
)
from tracker.runtime.native_output import discover_native_files, valid_native_path
from tracker.sandbox import runtime_sandbox

router = APIRouter(prefix="/benchmarks")


def _load_task_or_404(benchmark_id: UUID, task_id: str, org: Org, session: Session) -> tuple[Benchmark, Task]:
    """Return (benchmark, task) scoped to org, 404 if either is missing."""
    benchmark = session.exec(
        select(Benchmark).where(Benchmark.id == benchmark_id).where(Benchmark.org_id == org.id)
    ).first()

    if benchmark is None:
        raise HTTPException(status_code=404, detail="Benchmark not found")

    return benchmark, load_task_for_benchmark_or_404(benchmark, task_id, org, session)


def _task_prefix(benchmark_id: UUID, task_id: str) -> str:
    """S3 prefix for a task's artifacts (presigned URLs + run outputs)."""
    return f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{task_id}/"


def _fetch_result_objects(
    session: Session, task: Task, org: Org
) -> tuple[EvaluationResult | None, tuple[str, FailureCategory | None] | None]:
    """Fetches a task's evaluation result or error message depending on its status."""
    if task.status not in (TaskStatus.FINISHED, TaskStatus.ERROR):
        return None, None

    result_model = EvaluationResult if task.status == TaskStatus.FINISHED else ErrorResult
    result_filters = (
        result_model.task == task.id,
        result_model.org_id == org.id,
    )
    result_order = desc(result_model.created_at)

    if task.status == TaskStatus.FINISHED:
        result_select = select(EvaluationResult)
    else:
        result_select = select(ErrorResult.error_message, ErrorResult.category).where(
            col(ErrorResult.retry_scheduled).is_(False)
        )

    result = session.exec(result_select.where(*result_filters).order_by(result_order)).first()

    if task.status == TaskStatus.FINISHED:
        return cast(EvaluationResult | None, result), None

    return None, cast(tuple[str, FailureCategory | None] | None, result)


@router.get(
    "/{benchmark_id}/tasks/{task_id}",
    response_model=SingleTaskResponse,
)
def get_single_task(
    benchmark_id: TrackedBenchmarkId,
    task_id: str,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> SingleTaskResponse:
    """Fetch a single task's status + evaluation result for the SingleTask page."""
    _, task = _load_task_or_404(benchmark_id, task_id, org, session)

    eval_row, error = _fetch_result_objects(session, task, org)
    task_breakdown_row = session.get(TaskBreakdownRow, task.task_breakdown) if task.task_breakdown is not None else None

    return SingleTaskResponse(
        id=task.id,
        task_id=task.task_id,
        status=task.status,
        started_at=task.started_at,
        finished_at=task.finished_at,
        error_message=error[0] if error else None,
        failure_category=error[1] if error else None,
        evaluation_result=eval_row.result if eval_row else None,
        agent_caused_exit_reason=(
            eval_row.agent_caused_exit_reason.value if eval_row and eval_row.agent_caused_exit_reason else None
        ),
        task_breakdown=(
            TaskBreakdownResponse.model_validate(task_breakdown_row) if task_breakdown_row is not None else None
        ),
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id}/artifacts",
    response_model=TaskArtifactsResponse,
)
async def get_task_artifacts(
    benchmark_id: TrackedBenchmarkId,
    task_id: str,
    benchmark: RunBenchmarkDependency,
    runtime: RunRuntimeDependency,
    request: Request,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TaskArtifactsResponse:
    """Return log and agent output locations for the task detail page."""
    task = load_task_for_benchmark_or_404(benchmark, task_id, org, session)
    cloudwatch_url: str | None
    if benchmark.arguments.environment == "local":
        cloudwatch_url = str(
            request.url_for("get_logs", benchmark_id=benchmark_id).include_query_params(task_id=task_id)
        )
    elif isinstance(runtime, CloudRuntimeServices) and not (
        runtime.aws_runtime.resources.log_group and runtime.aws_runtime.resources.region
    ):
        cloudwatch_url = None
    elif any(character in task.task_id for character in ":*%"):
        # Renamed streams may use either encoding; link to the run without guessing.
        cloudwatch_url = runtime.log_locations.benchmark_location(str(benchmark_id))
    else:
        cloudwatch_url = runtime.log_locations.task_location(
            str(benchmark_id), task_log_stream_name(task.task_id, task.started_at)
        )

    agent_output_url: str | None = None
    ttl_seconds: int | None = None
    key = f"{_task_prefix(benchmark_id, task_id)}agent_output.tar.gz"
    if await runtime.objects.exists(key):
        agent_output_url, ttl_seconds = await resolve_download_url(
            runtime.objects,
            key,
            request=request,
            route_name="get_run_artifact_url",
            route_params={"benchmark_id": benchmark_id},
            query_params={"path": f"{task_id}/agent_output.tar.gz"},
            expires_in=300,
        )

    return TaskArtifactsResponse(
        cloudwatch_url=cloudwatch_url,
        agent_output_url=agent_output_url,
        agent_output_expires_in=ttl_seconds,
    )


def _active_native_output(task: Task) -> ActiveNativeOutput | None:
    descriptor = task.active_native_output
    if (
        descriptor is None
        or task.status not in (TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
        or not descriptor.matches_attempt(task.started_at)
    ):
        return None
    return descriptor


def _recheck_native_generation(session: Session, task: Task, generation: str) -> None:
    """Check the current attempt in a new read transaction, without holding it across sandbox I/O."""
    try:
        session.refresh(task)
        current = _active_native_output(task)
        if current is None or current.generation != generation:
            raise HTTPException(status_code=409, detail="Native output generation changed")
    finally:
        session.rollback()


@router.get(
    "/{benchmark_id}/tasks/{task_id}/native-output/files",
    response_model=NativeOutputFilesResponse,
    responses={
        204: {"description": "No active native output target"},
        404: {"description": "Task not found"},
        409: {"description": "Native output generation changed"},
    },
)
async def get_native_output_files(
    benchmark_id: TrackedBenchmarkId,
    task_id: str,
    benchmark: RunBenchmarkDependency,
    runtime: RunRuntimeDependency,
    response: Response,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> NativeOutputFilesResponse | Response:
    """List current native files directly from the exact execution sandbox."""
    task = load_task_for_benchmark_or_404(benchmark, task_id, org, session)
    descriptor = _active_native_output(task)
    if descriptor is None:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    # The descriptor is a value object; no ORM state is needed during sandbox I/O.
    session.rollback()

    config = await runtime.get_sandbox_provider_config()
    try:
        async with config.create_provider() as provider:
            sandbox = runtime_sandbox(await provider.get_sandbox(descriptor.sandbox_id), descriptor.runtime_source)
            files = await discover_native_files(sandbox, descriptor.output_path)
    except SandboxNotFoundError:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    _recheck_native_generation(session, task, descriptor.generation)
    if files is None:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})

    response.headers["Cache-Control"] = "no-store"
    return NativeOutputFilesResponse(
        generation=descriptor.generation,
        files=[NativeOutputFile(path=path, size_bytes=file.size_bytes) for path, file in sorted(files.items())],
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id}/native-output/file",
    response_class=Response,
    responses={
        200: {
            "content": {"application/octet-stream": {"schema": {"type": "string", "format": "binary"}}},
            "headers": {"X-Native-Output-Generation": {"schema": {"type": "string"}}},
        },
        204: {"description": "No active native output target"},
        400: {"description": "Malformed relative path"},
        404: {"description": "Task or native file not found"},
        409: {"description": "Native output generation changed"},
    },
)
async def get_native_output_file(
    benchmark_id: TrackedBenchmarkId,
    task_id: str,
    path: str,
    generation: str,
    benchmark: RunBenchmarkDependency,
    runtime: RunRuntimeDependency,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> Response:
    """Read one allowed file and discard bytes if its execution generation changes."""
    task = load_task_for_benchmark_or_404(benchmark, task_id, org, session)
    if not valid_native_path(path):
        raise HTTPException(status_code=400, detail="Expected a nonempty POSIX-relative file path")
    descriptor = _active_native_output(task)
    if descriptor is None:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    if descriptor.generation != generation:
        raise HTTPException(status_code=409, detail="Native output generation changed")
    session.rollback()

    config = await runtime.get_sandbox_provider_config()
    try:
        async with config.create_provider() as provider:
            sandbox = runtime_sandbox(await provider.get_sandbox(descriptor.sandbox_id), descriptor.runtime_source)
            files = await discover_native_files(sandbox, descriptor.output_path)
            _recheck_native_generation(session, task, generation)
            if files is None:
                return Response(status_code=204, headers={"Cache-Control": "no-store"})
            if path not in files:
                raise HTTPException(status_code=404, detail="Native file not found")
            content = await sandbox.download_file(files[path].remote_path)
    except SandboxNotFoundError:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    _recheck_native_generation(session, task, generation)

    return Response(
        content=content,
        media_type="application/octet-stream",
        headers={"X-Native-Output-Generation": generation, "Cache-Control": "no-store"},
    )
