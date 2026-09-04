"""Authenticated benchmark log access endpoints."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import suppress
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlmodel import Session, col, select

from tracker.api.dependencies import (
    LogProviderDependency,
    RunAWSDependency,
    load_task_for_benchmark_or_404,
)
from tracker.auth import get_current_org
from tracker.database.models import Org, Task
from tracker.database.session import get_session
from tracker.runtime.logs import (
    LogEvent,
    LogPage,
    LogProvider,
    LogProviderError,
    RunLogReference,
    RunTaskLogReference,
    TaskLogReference,
)
from tracker.types import LogEventResponse, LogPageResponse


def _validate_log_filters(
    _run_context: RunAWSDependency,
    query: str | None = Query(default=None, min_length=1),
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> None:
    """Reject ambiguous log filters before handling a request."""
    if query is not None and not query.strip():
        raise HTTPException(status_code=422, detail="query must not be blank")
    for name, value in (("start_time", start_time), ("end_time", end_time)):
        if value is not None and value.utcoffset() is None:
            raise HTTPException(status_code=422, detail=f"{name} must include a timezone offset")
    if start_time is not None and end_time is not None and end_time <= start_time:
        raise HTTPException(status_code=422, detail="end_time must be later than start_time")


router = APIRouter(prefix="/benchmarks", dependencies=[Depends(_validate_log_filters)])


def _task_reference(
    run_context: RunAWSDependency,
    task_id: str = Query(min_length=1),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TaskLogReference:
    task = load_task_for_benchmark_or_404(run_context.benchmark, task_id, org, session)
    run_tasks = _run_tasks(run_context, org, session)
    return TaskLogReference(
        run_id=run_context.benchmark.id,
        task_id=task.task_id,
        started_at=task.started_at,
        siblings=tuple(run_task for run_task in run_tasks if run_task.task_id != task.task_id),
    )


TaskLogReferenceDependency = Annotated[TaskLogReference, Depends(_task_reference)]


def _run_tasks(
    run_context: RunAWSDependency,
    org: Org,
    session: Session,
) -> tuple[RunTaskLogReference, ...]:
    tasks = session.exec(
        select(Task)
        .where(col(Task.benchmark) == run_context.benchmark.id)
        .where(col(Task.org_id) == org.id)
        .order_by(col(Task.task_id))
    ).all()
    return tuple(RunTaskLogReference(task_id=task.task_id, started_at=task.started_at) for task in tasks)


def _run_reference(
    run_context: RunAWSDependency,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> RunLogReference:
    return RunLogReference(
        run_id=run_context.benchmark.id,
        tasks=_run_tasks(run_context, org, session),
    )


def _log_reference(
    run_context: RunAWSDependency,
    task_id: str | None = Query(default=None, min_length=1),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> RunLogReference | TaskLogReference:
    if task_id is not None:
        return _task_reference(run_context, task_id, org, session)
    return _run_reference(run_context, org, session)


LogReferenceDependency = Annotated[RunLogReference | TaskLogReference, Depends(_log_reference)]


def _response(page: LogPage) -> LogPageResponse:
    return LogPageResponse(
        events=[_event_response(event) for event in page.events],
        next_cursor=page.next_cursor,
    )


def _event_response(event: LogEvent) -> LogEventResponse:
    return LogEventResponse(
        timestamp=event.timestamp,
        message=event.message,
        task_id=event.task_id,
        ingestion_time=event.ingestion_time,
        event_id=event.event_id,
    )


async def _next_log_event(iterator: AsyncIterator[LogEvent]) -> LogEvent:
    return await anext(iterator)


async def _stream_events(
    log_provider: LogProvider,
    reference: TaskLogReference,
    *,
    query: str | None,
    start_time: datetime | None,
    end_time: datetime | None,
    keep_alive_interval: float = 15.0,
) -> AsyncGenerator[str, None]:
    yield ": connected\n\n"
    iterator = log_provider.stream_task(
        reference,
        query=query,
        start_time=start_time,
        end_time=end_time,
    ).__aiter__()
    pending_event: asyncio.Task[LogEvent] | None = asyncio.create_task(_next_log_event(iterator))
    try:
        while pending_event is not None:
            done, _ = await asyncio.wait({pending_event}, timeout=keep_alive_interval)
            if not done:
                yield ": keep-alive\n\n"
                continue

            completed_event = pending_event
            pending_event = None
            try:
                event = completed_event.result()
            except StopAsyncIteration:
                break

            pending_event = asyncio.create_task(_next_log_event(iterator))
            payload = _event_response(event).model_dump_json()
            yield f"event: log\ndata: {payload}\n\n"
    except LogProviderError as error:
        payload = json.dumps({"detail": str(error)})
        yield f"event: error\ndata: {payload}\n\n"
        return
    finally:
        if pending_event is not None:
            pending_event.cancel()
            with suppress(asyncio.CancelledError, LogProviderError, StopAsyncIteration):
                await pending_event
    yield "event: end\ndata: {}\n\n"


@router.get("/{benchmark_id}/logs", response_model=LogPageResponse)
async def get_logs(
    reference: LogReferenceDependency,
    log_provider: LogProviderDependency,
    query: str | None = Query(default=None, min_length=1),
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    cursor: str | None = None,
    limit: int = Query(default=1_000, ge=1, le=10_000),
) -> LogPageResponse:
    """Return one page of logs for a run or one of its tasks."""
    try:
        page = await log_provider.fetch(
            reference,
            query=query,
            start_time=start_time,
            end_time=end_time,
            cursor=cursor,
            limit=limit,
        )
    except LogProviderError as error:
        raise HTTPException(status_code=502, detail=str(error)) from error
    return _response(page)


@router.get("/{benchmark_id}/logs/stream")
async def stream_task_logs(
    reference: TaskLogReferenceDependency,
    log_provider: LogProviderDependency,
    query: str | None = Query(default=None, min_length=1),
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> StreamingResponse:
    """Stream a task's current log stream as server-sent events."""
    events = _stream_events(
        log_provider,
        reference,
        query=query,
        start_time=start_time,
        end_time=end_time,
    )
    return StreamingResponse(events, media_type="text/event-stream")
