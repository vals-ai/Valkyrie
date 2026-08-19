"""Per-task drill-in endpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, cast
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, col, desc, select

from tracker.api.dependencies import RunAWSDependency
from tracker.auth import get_current_org
from tracker.aws.cloudwatch_logs import get_benchmark_log_url, sanitize_log_stream_name
from tracker.aws.s3 import S3_BENCHMARKS_PREFIX, create_presigned_url, s3_object_exists
from tracker.database.models import (
    Benchmark,
    ErrorResult,
    EvaluationResult,
    Org,
    Task,
    TaskStatus,
)
from tracker.database.session import get_session
from tracker.types import (
    SingleTaskResponse,
    TaskArtifactsResponse,
    TaskLogAttempt,
    TaskLogAttemptsResponse,
    TaskLogEvent,
    TaskLogEventsResponse,
)

router = APIRouter(prefix="/benchmarks")
_MAX_LOG_ATTEMPTS = 2_000
_MAX_LOG_RESPONSE_BYTES = 1_900_000


def _load_task_or_404(benchmark_id: UUID, task_id: str, org: Org, session: Session) -> tuple[Benchmark, Task]:
    """Return (benchmark, task) scoped to org, 404 if either is missing."""
    benchmark = session.exec(
        select(Benchmark).where(Benchmark.id == benchmark_id).where(Benchmark.org_id == org.id)
    ).first()

    if benchmark is None:
        raise HTTPException(status_code=404, detail="Benchmark not found")

    return benchmark, _load_task_for_benchmark_or_404(benchmark, task_id, org, session)


def _load_task_for_benchmark_or_404(benchmark: Benchmark, task_id: str, org: Org, session: Session) -> Task:
    """Return a task from an already organization-scoped benchmark."""
    task = session.exec(
        select(Task).where(Task.benchmark == benchmark.id).where(Task.org_id == org.id).where(Task.task_id == task_id)
    ).first()

    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")

    return task


def _task_prefix(benchmark_id: UUID, task_id: str) -> str:
    """S3 prefix for a task's artifacts (presigned URLs + run outputs)."""
    return f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{task_id}/"


def _fetch_result_objects(session: Session, task: Task, org: Org) -> tuple[EvaluationResult | None, str | None]:
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
        result_select = select(ErrorResult.error_message).where(col(ErrorResult.retry_scheduled).is_(False))

    result = session.exec(result_select.where(*result_filters).order_by(result_order)).first()

    if task.status == TaskStatus.FINISHED:
        return cast(EvaluationResult | None, result), None

    return None, cast(str | None, result)


@router.get(
    "/{benchmark_id}/tasks/{task_id}",
    response_model=SingleTaskResponse,
)
def get_single_task(
    benchmark_id: UUID,
    task_id: str,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> SingleTaskResponse:
    """Fetch a single task's status + evaluation result for the SingleTask page."""
    _, task = _load_task_or_404(benchmark_id, task_id, org, session)

    eval_row, error_message = _fetch_result_objects(session, task, org)

    return SingleTaskResponse(
        id=task.id,
        task_id=task.task_id,
        status=task.status,
        started_at=task.started_at,
        finished_at=task.finished_at,
        error_message=error_message,
        evaluation_result=eval_row.result if eval_row else None,
        agent_caused_exit_reason=(
            eval_row.agent_caused_exit_reason.value if eval_row and eval_row.agent_caused_exit_reason else None
        ),
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id}/artifacts",
    response_model=TaskArtifactsResponse,
)
async def get_task_artifacts(
    benchmark_id: UUID,
    task_id: str,
    run_context: RunAWSDependency,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TaskArtifactsResponse:
    """CloudWatch URL + presigned URL for the agent's output tarball, for the SingleTask page."""
    task = _load_task_for_benchmark_or_404(run_context.benchmark, task_id, org, session)
    aws_runtime = run_context.aws_runtime

    cloudwatch_url: str | None = None
    if aws_runtime.resources.log_group and aws_runtime.resources.region:
        log_stream_suffix = f"{int(task.started_at.timestamp() * 1_000_000):x}"
        cloudwatch_url = get_benchmark_log_url(
            benchmark_id=str(benchmark_id),
            resources=aws_runtime.resources,
            task_id=f"{task.task_id}_{log_stream_suffix}",
        )

    agent_output_url: str | None = None
    ttl_seconds: int | None = None
    key = f"{_task_prefix(benchmark_id, task_id)}agent_output.tar.gz"
    if await s3_object_exists(key, aws_runtime):
        ttl_seconds = aws_runtime.clients.maximum_presign_ttl(300)
        agent_output_url = await create_presigned_url(
            s3_key=key,
            runtime=aws_runtime,
            expiration=ttl_seconds,
        )

    return TaskArtifactsResponse(
        cloudwatch_url=cloudwatch_url,
        agent_output_url=agent_output_url,
        agent_output_expires_in=ttl_seconds,
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id:path}/logs/attempts",
    response_model=TaskLogAttemptsResponse,
)
def get_task_log_attempts(
    benchmark_id: UUID,
    task_id: str,
    run_context: RunAWSDependency,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TaskLogAttemptsResponse:
    """List CloudWatch log attempts through the run's persisted AWS runtime."""
    task = _load_task_for_benchmark_or_404(run_context.benchmark, task_id, org, session)
    runtime = run_context.aws_runtime
    client = runtime.clients.cloudwatch_logs_client()
    prefix = sanitize_log_stream_name(task.task_id)
    current_stream = _task_log_stream_name(task)
    reserved_stream_names = _reserved_log_stream_names(session, run_context.benchmark.id, org.id)
    streams: list[dict[str, object]] = []
    token: str | None = None

    try:
        while len(streams) < _MAX_LOG_ATTEMPTS:
            request: dict[str, object] = {
                "logGroupName": _log_group_name(benchmark_id, runtime.resources.log_group),
                "logStreamNamePrefix": prefix,
                "limit": 50,
            }
            if token:
                request["nextToken"] = token
            response = client.describe_log_streams(**request)
            streams.extend(cast(list[dict[str, object]], response.get("logStreams", [])))
            token = cast(str | None, response.get("nextToken"))
            if not token:
                break
    except (BotoCoreError, ClientError) as error:
        if _is_missing_log_group(error):
            return TaskLogAttemptsResponse(attempts=[], truncated=False)
        raise HTTPException(status_code=502, detail="Task logs could not be loaded.") from error

    attempts = [
        attempt
        for stream in streams[:_MAX_LOG_ATTEMPTS]
        if (attempt := _task_log_attempt(stream, prefix, current_stream, reserved_stream_names)) is not None
    ]
    attempts.sort(key=lambda attempt: attempt.started_at, reverse=True)
    return TaskLogAttemptsResponse(
        attempts=attempts,
        truncated=token is not None or len(streams) > _MAX_LOG_ATTEMPTS,
    )


@router.get(
    "/{benchmark_id}/tasks/{task_id:path}/logs/events",
    response_model=TaskLogEventsResponse,
)
def get_task_log_events(
    benchmark_id: UUID,
    task_id: str,
    run_context: RunAWSDependency,
    attempt_id: str = Query(pattern=r"^[0-9a-f]{1,16}$"),
    direction: Literal["initial", "older", "newer"] = "initial",
    cursor: str | None = Query(default=None, max_length=4_096),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TaskLogEventsResponse:
    """Read one bounded CloudWatch page through the run's persisted AWS runtime."""
    task = _load_task_for_benchmark_or_404(run_context.benchmark, task_id, org, session)
    if direction != "initial" and cursor is None:
        raise HTTPException(status_code=400, detail="A cursor is required for this log direction.")
    if direction == "initial" and cursor is not None:
        raise HTTPException(status_code=400, detail="Initial log reads cannot include a cursor.")

    runtime = run_context.aws_runtime
    stream_name = _attempt_stream_name(sanitize_log_stream_name(task.task_id), attempt_id)
    if stream_name in _reserved_log_stream_names(session, run_context.benchmark.id, org.id):
        raise HTTPException(status_code=404, detail="Task log attempt not found.")
    request: dict[str, object] = {
        "logGroupName": _log_group_name(benchmark_id, runtime.resources.log_group),
        "logStreamName": stream_name,
        "startFromHead": direction == "newer",
    }
    if cursor:
        request["nextToken"] = cursor

    limit = 100
    try:
        while True:
            response = runtime.clients.cloudwatch_logs_client().get_log_events(**request, limit=limit)
            result = _task_log_events(response, direction, cursor)
            if len(result.model_dump_json().encode()) <= _MAX_LOG_RESPONSE_BYTES:
                return result
            if limit == 1:
                raise HTTPException(status_code=502, detail="A task log event is too large.")
            limit = max(1, limit // 2)
    except (BotoCoreError, ClientError) as error:
        if _is_missing_log_group(error):
            return TaskLogEventsResponse(events=[], older_cursor=None, newer_cursor=None)
        raise HTTPException(status_code=502, detail="Task logs could not be loaded.") from error


def _reserved_log_stream_names(session: Session, benchmark_id: UUID, org_id: UUID) -> set[str]:
    task_ids = session.exec(
        select(Task.task_id).where(Task.benchmark == benchmark_id).where(Task.org_id == org_id)
    ).all()
    return {sanitize_log_stream_name(task_id) for task_id in task_ids}


def _task_log_stream_name(task: Task) -> str:
    suffix = f"{int(task.started_at.timestamp() * 1_000_000):x}"
    return f"{sanitize_log_stream_name(task.task_id)}_{suffix}"


def _attempt_stream_name(prefix: str, attempt_id: str) -> str:
    return f"{prefix}_{attempt_id}"


def _log_group_name(benchmark_id: UUID, root: str) -> str:
    return f"{root}/{benchmark_id}"


def _task_log_attempt(
    stream: dict[str, object],
    prefix: str,
    current_stream: str,
    reserved_stream_names: set[str],
) -> TaskLogAttempt | None:
    name = stream.get("logStreamName")
    if not isinstance(name, str) or name in reserved_stream_names:
        return None
    attempt_id = _attempt_id(prefix, name)
    if attempt_id is None:
        return None
    started_at = _attempt_started_at(attempt_id)
    if started_at is None:
        return None
    return TaskLogAttempt(
        id=attempt_id,
        started_at=started_at,
        first_event_at=_event_time(stream.get("firstEventTimestamp")),
        last_event_at=_event_time(stream.get("lastEventTimestamp")),
        current=name == current_stream,
    )


def _attempt_id(prefix: str, stream_name: str) -> str | None:
    marker = f"{prefix}_"
    if not stream_name.startswith(marker):
        return None
    suffix = stream_name[len(marker) :]
    if not 1 <= len(suffix) <= 16:
        return None
    return suffix if all(character in "0123456789abcdef" for character in suffix) else None


def _attempt_started_at(attempt_id: str) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(attempt_id, 16) / 1_000_000, timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def _task_log_events(
    response: dict[str, object],
    direction: Literal["initial", "older", "newer"],
    cursor: str | None,
) -> TaskLogEventsResponse:
    events: list[TaskLogEvent] = []
    for event in cast(list[dict[str, object]], response.get("events", [])):
        timestamp = _event_time(event.get("timestamp"))
        message = event.get("message")
        if timestamp is None or not isinstance(message, str):
            raise HTTPException(status_code=502, detail="CloudWatch returned an incomplete log event.")
        events.append(
            TaskLogEvent(
                timestamp=timestamp,
                ingestion_time=_event_time(event.get("ingestionTime")),
                message=message,
            )
        )
    older_cursor = cast(str | None, response.get("nextBackwardToken"))
    newer_cursor = cast(str | None, response.get("nextForwardToken"))
    if direction == "older" and older_cursor == cursor:
        older_cursor = None
    return TaskLogEventsResponse(events=events, older_cursor=older_cursor, newer_cursor=newer_cursor)


def _event_time(value: object) -> datetime | None:
    if not isinstance(value, int | float):
        return None
    return datetime.fromtimestamp(value / 1_000, timezone.utc)


def _is_missing_log_group(error: BotoCoreError | ClientError) -> bool:
    return isinstance(error, ClientError) and error.response.get("Error", {}).get("Code") == "ResourceNotFoundException"
