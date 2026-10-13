"""Authenticated benchmark log access endpoints."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import suppress
from datetime import datetime
from typing import Annotated, TypedDict

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlmodel import Session, col, select

from tracker.api.dependencies import (
    RunRuntimeDependency,
    RunBenchmarkDependency,
    load_task_for_benchmark_or_404,
)
from tracker.auth import get_current_org
from tracker.database.models import Benchmark, Org, Task
from tracker.database.session import get_session
from tracker.runtime.logs import (
    LogProvider,
    LogProviderError,
    RunLogReference,
    RunTaskLogReference,
    TaskLogReference,
)
from tracker.types import LogEventResponse, LogPageResponse


class LogFilters(TypedDict):
    query: str | None
    start_time: datetime | None
    end_time: datetime | None


def _log_filters(
    _benchmark: RunBenchmarkDependency,
    query: str | None = Query(default=None, min_length=1),
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> LogFilters:
    """Reject ambiguous log filters before handling a request."""
    if query is not None and not query.strip():
        raise HTTPException(status_code=422, detail="query must not be blank")
    for name, value in (("start_time", start_time), ("end_time", end_time)):
        if value is not None and value.utcoffset() is None:
            raise HTTPException(status_code=422, detail=f"{name} must include a timezone offset")
    if start_time is not None and end_time is not None and end_time <= start_time:
        raise HTTPException(status_code=422, detail="end_time must be later than start_time")
    return LogFilters(query=query, start_time=start_time, end_time=end_time)


LogFiltersDependency = Annotated[LogFilters, Depends(_log_filters)]

router = APIRouter(prefix="/benchmarks")


def _task_reference(
    benchmark: RunBenchmarkDependency,
    task_id: str = Query(min_length=1),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> TaskLogReference:
    task = load_task_for_benchmark_or_404(benchmark, task_id, org, session)
    run_tasks = _run_tasks(benchmark, org, session)
    return TaskLogReference(
        run_id=benchmark.id,
        task_id=task.task_id,
        started_at=task.started_at,
        siblings=tuple(run_task for run_task in run_tasks if run_task.task_id != task.task_id),
    )


TaskLogReferenceDependency = Annotated[TaskLogReference, Depends(_task_reference)]


def _run_tasks(
    benchmark: Benchmark,
    org: Org,
    session: Session,
) -> tuple[RunTaskLogReference, ...]:
    tasks = session.exec(
        select(Task)
        .where(col(Task.benchmark) == benchmark.id)
        .where(col(Task.org_id) == org.id)
        .order_by(col(Task.task_id))
    ).all()
    return tuple(RunTaskLogReference(task_id=task.task_id, started_at=task.started_at) for task in tasks)


def _run_reference(
    benchmark: RunBenchmarkDependency,
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> RunLogReference:
    return RunLogReference(
        run_id=benchmark.id,
        tasks=_run_tasks(benchmark, org, session),
    )


def _log_reference(
    benchmark: RunBenchmarkDependency,
    task_id: str | None = Query(default=None, min_length=1),
    org: Org = Depends(get_current_org),
    session: Session = Depends(get_session),
) -> RunLogReference | TaskLogReference:
    if task_id is not None:
        return _task_reference(benchmark, task_id, org, session)
    return _run_reference(benchmark, org, session)


LogReferenceDependency = Annotated[RunLogReference | TaskLogReference, Depends(_log_reference)]


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
    pending_event = asyncio.ensure_future(anext(iterator))
    try:
        while True:
            done, _ = await asyncio.wait({pending_event}, timeout=keep_alive_interval)
            if not done:
                yield ": keep-alive\n\n"
                continue
            try:
                event = pending_event.result()
            except StopAsyncIteration:
                break
            pending_event = asyncio.ensure_future(anext(iterator))
            payload = LogEventResponse.model_validate(event, from_attributes=True).model_dump_json()
            yield f"event: log\ndata: {payload}\n\n"
    except LogProviderError as error:
        payload = json.dumps({"detail": str(error)})
        yield f"event: error\ndata: {payload}\n\n"
        return
    finally:
        pending_event.cancel()
        with suppress(asyncio.CancelledError, LogProviderError, StopAsyncIteration):
            await pending_event
    yield "event: end\ndata: {}\n\n"


@router.get("/{benchmark_id}/logs", response_model=LogPageResponse)
async def get_logs(
    filters: LogFiltersDependency,
    reference: LogReferenceDependency,
    runtime: RunRuntimeDependency,
    cursor: str | None = None,
    limit: int = Query(default=1_000, ge=1, le=10_000),
) -> LogPageResponse:
    """Return one page of logs for a run or one of its tasks."""
    try:
        page = await runtime.log_reader.fetch(reference, **filters, cursor=cursor, limit=limit)
    except LogProviderError as error:
        raise HTTPException(status_code=502, detail=str(error)) from error
    return LogPageResponse.model_validate(page, from_attributes=True)


@router.get("/{benchmark_id}/logs/stream")
def stream_task_logs(
    filters: LogFiltersDependency,
    reference: TaskLogReferenceDependency,
    runtime: RunRuntimeDependency,
    session: Session = Depends(get_session),
) -> StreamingResponse:
    """Stream a task's current log stream as server-sent events."""
    # References are materialized; release the connection before long-lived polling.
    session.close()
    return StreamingResponse(_stream_events(runtime.log_reader, reference, **filters), media_type="text/event-stream")
