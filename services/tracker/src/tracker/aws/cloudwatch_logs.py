from __future__ import annotations

import asyncio
import hashlib
import re
import time
from collections import deque
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import datetime, timezone
from functools import wraps
from math import floor
from typing import Any, ParamSpec, TypeVar, cast
from urllib.parse import quote

import logfire
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws.clients import AWSClientProvider
from tracker.aws.runtime import AWSResources
from tracker.exceptions import CloudWatchError
from tracker.runtime.logs import (
    BenchmarkLogLocations,
    BenchmarkLogSink,
    LogEvent,
    LogPage,
    LogProvider,
    LogProviderError,
    RunLogReference,
    TaskLogReference,
)

_created_streams: set[str] = set()
_FOLLOW_DEDUPLICATION_WINDOW = 10_000
_P = ParamSpec("_P")
_R = TypeVar("_R")


def _sanitize_log_stream_name(task_id: str) -> str:
    """Make a task ID safe to use as a CloudWatch log stream name."""
    return re.sub(r"[:*]", "_", task_id)


def benchmark_log_group_name(log_group: str, benchmark_id: str) -> str:
    """Return the canonical CloudWatch log group for a benchmark run."""
    return f"{log_group}/{benchmark_id}"


def task_log_stream_name(task_id: str, started_at: datetime) -> str:
    """Return the canonical versioned CloudWatch stream for a task attempt."""
    if started_at.utcoffset() is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    suffix = f"{int(started_at.timestamp() * 1_000_000):x}"
    return _sanitize_log_stream_name(f"{task_id}_{suffix}")


def handle_cloudwatch_error(message: str) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    def decorator(func: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(func)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return func(*args, **kwargs)
            except (ClientError, BotoCoreError) as error:
                raise CloudWatchError(f"{message}: {error}") from error

        return wrapper

    return decorator


class CloudWatchBenchmarkLogLocations(BenchmarkLogLocations):
    """CloudWatch console locations for resolved AWS resources."""

    def __init__(self, resources: AWSResources) -> None:
        self._resources = resources

    def benchmark_location(self, benchmark_id: str) -> str:
        return self._location(benchmark_id)

    def task_location(self, benchmark_id: str, task_stream_id: str) -> str:
        return self._location(benchmark_id, task_stream_id)

    def _location(self, benchmark_id: str, task_stream_id: str | None = None) -> str:
        safe_region = quote(self._resources.region, safe="-")
        safe_log_group = quote(self._resources.log_group, safe="-_")
        safe_benchmark_id = quote(benchmark_id, safe="-_")
        base = f"https://{safe_region}.console.aws.amazon.com/cloudwatch/home?region={safe_region}"
        encoded_log_group = f"{safe_log_group}$252F{safe_benchmark_id}"
        if task_stream_id:
            safe_task_id = quote(_sanitize_log_stream_name(task_stream_id), safe="-_.")
            return f"{base}#logsV2:log-groups/log-group/{encoded_log_group}/log-events/{safe_task_id}"
        return f"{base}#logsV2:log-groups/log-group/{encoded_log_group}"


class CloudWatchBenchmarkLogSink(BenchmarkLogSink):
    """CloudWatch transport using already-resolved AWS authority."""

    def __init__(self, clients: AWSClientProvider, log_group: str) -> None:
        self._clients = clients
        self._log_group = log_group

    @handle_cloudwatch_error(message="Failed to create log group")
    @logfire.instrument("create_log_group", extract_args=("benchmark_id",))
    def create_benchmark(self, benchmark_id: str, *, retention_days: int) -> None:
        client = self._clients.cloudwatch_logs_client()
        log_group_name = benchmark_log_group_name(self._log_group, benchmark_id)
        try:
            client.create_log_group(logGroupName=log_group_name)  # pyright: ignore[reportUnknownMemberType]
            client.put_retention_policy(  # pyright: ignore[reportUnknownMemberType]
                logGroupName=log_group_name,
                retentionInDays=retention_days,
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
                raise

    @handle_cloudwatch_error(message="Failed to create cloudwatch stream")
    def write(self, stream_key: str, message: str) -> None:
        if not message.strip():
            return

        benchmark_id, task_id = stream_key.split(":", 1)
        if not benchmark_id or not task_id:
            raise CloudWatchError(f"Invalid stream key '{stream_key}', expected format 'benchmark_id:task_id'")

        client = self._clients.cloudwatch_logs_client()
        log_group_name = benchmark_log_group_name(self._log_group, benchmark_id)
        stream_name = _sanitize_log_stream_name(task_id)

        if stream_key not in _created_streams:
            try:
                client.create_log_stream(logGroupName=log_group_name, logStreamName=stream_name)  # pyright: ignore[reportUnknownMemberType]
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
                    raise
            except BotoCoreError as error:
                raise CloudWatchError(f"Failed to create log stream '{stream_name}': {error}") from error
            _created_streams.add(stream_key)

        try:
            client.put_log_events(  # pyright: ignore[reportUnknownMemberType]
                logGroupName=log_group_name,
                logStreamName=stream_name,
                logEvents=[{"timestamp": int(time.time() * 1000), "message": message}],
            )
        except (ClientError, BotoCoreError) as error:
            raise CloudWatchError(f"Failed to put log event: {error}") from error


class CloudWatchLogProvider(LogProvider):
    """Read benchmark logs from CloudWatch without exposing AWS details to callers."""

    def __init__(self, clients: AWSClientProvider, log_group: str) -> None:
        self._clients = clients
        self._log_group = log_group

    async def fetch_task(
        self,
        reference: TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 1_000,
    ) -> LogPage:
        """Return one filtered page from a task's current stream."""
        stream_name = task_log_stream_name(reference.task_id, reference.started_at)
        response = await self._filter_events(
            str(reference.run_id),
            stream_names=[stream_name],
            start_time=start_time,
            end_time=end_time,
            cursor=cursor,
            limit=limit,
        )
        return self._page(response, cursor=cursor, query=query, task_id=reference.task_id)

    async def fetch_run(
        self,
        reference: RunLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 1_000,
    ) -> LogPage:
        """Return one filtered page across every stream in a run's log group."""
        response = await self._filter_events(
            str(reference.run_id),
            stream_names=None,
            start_time=start_time,
            end_time=end_time,
            cursor=cursor,
            limit=limit,
        )
        return self._page(
            response,
            cursor=cursor,
            query=query,
            stream_task_ids=_stream_task_ids(reference),
        )

    async def stream_task(
        self,
        reference: TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        poll_interval: float = 1.0,
    ) -> AsyncIterator[LogEvent]:
        """Yield a task stream from its current start position, then poll for new events."""
        client = await self._get_client()
        log_group_name = benchmark_log_group_name(self._log_group, str(reference.run_id))
        stream_name = task_log_stream_name(reference.task_id, reference.started_at)
        cursor: str | None = None
        recent_event_ids: deque[str] = deque()
        seen_event_ids: set[str] = set()

        while True:
            request: dict[str, Any] = {
                "logGroupName": log_group_name,
                "logStreamName": stream_name,
                "startFromHead": True,
                "limit": 10_000,
            }
            if cursor is not None:
                request["nextToken"] = cursor
            elif start_time is not None:
                request["startTime"] = _epoch_milliseconds(start_time)
            if end_time is not None:
                request["endTime"] = _epoch_milliseconds(end_time)

            response = await self._request(client.get_log_events, request)
            if response is None:
                if end_time is not None and datetime.now(timezone.utc) > end_time:
                    return
                await asyncio.sleep(poll_interval)
                continue

            next_cursor = response.get("nextForwardToken")
            event_namespace = next_cursor if isinstance(next_cursor, str) else cursor
            events = _parse_events(
                response.get("events", []),
                task_id=reference.task_id,
                identity_namespace=event_namespace,
            )
            page_has_events = bool(events)
            if query is not None:
                events = [event for event in events if query in event.message]
            for event in sorted(events, key=_event_sort_key):
                event_id = event.event_id
                if event_id is not None and event_id in seen_event_ids:
                    continue
                if event_id is not None:
                    if len(recent_event_ids) == _FOLLOW_DEDUPLICATION_WINDOW:
                        seen_event_ids.discard(recent_event_ids.popleft())
                    recent_event_ids.append(event_id)
                    seen_event_ids.add(event_id)
                yield event

            stable_cursor = not isinstance(next_cursor, str) or next_cursor == cursor
            if isinstance(next_cursor, str):
                cursor = next_cursor

            if end_time is not None and stable_cursor and datetime.now(timezone.utc) > end_time:
                return
            if stable_cursor or not page_has_events:
                await asyncio.sleep(poll_interval)

    async def _filter_events(
        self,
        run_id: str,
        *,
        stream_names: list[str] | None,
        start_time: datetime | None,
        end_time: datetime | None,
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any] | None:
        request: dict[str, Any] = {
            "logGroupName": benchmark_log_group_name(self._log_group, run_id),
            "limit": limit,
        }
        if stream_names:
            request["logStreamNames"] = stream_names
        if start_time is not None:
            request["startTime"] = _epoch_milliseconds(start_time)
        if end_time is not None:
            request["endTime"] = _epoch_milliseconds(end_time)
        if cursor is not None:
            request["nextToken"] = cursor

        client = await self._get_client()
        return await self._request(client.filter_log_events, request)

    async def _get_client(self) -> Any:
        try:
            return await asyncio.to_thread(self._clients.cloudwatch_logs_client)
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            raise LogProviderError(f"CloudWatch log request failed: {code or 'unknown error'}") from error
        except BotoCoreError as error:
            raise LogProviderError("CloudWatch log request failed") from error

    async def _request(
        self,
        operation: Callable[..., Any],
        request: dict[str, Any],
    ) -> dict[str, Any] | None:
        try:
            response = await asyncio.to_thread(operation, **request)
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code")
            if code == "ResourceNotFoundException":
                return None
            raise LogProviderError(f"CloudWatch log request failed: {code or 'unknown error'}") from error
        except BotoCoreError as error:
            raise LogProviderError("CloudWatch log request failed") from error
        return cast(dict[str, Any], response)

    @staticmethod
    def _page(
        response: dict[str, Any] | None,
        *,
        cursor: str | None,
        query: str | None,
        task_id: str | None = None,
        stream_task_ids: Mapping[str, str | None] | None = None,
    ) -> LogPage:
        if response is None:
            return LogPage(events=[])

        events = _parse_events(response.get("events", []), task_id=task_id, stream_task_ids=stream_task_ids)
        if query is not None:
            events = [event for event in events if query in event.message]
        events.sort(key=_event_sort_key)
        next_cursor = response.get("nextToken")
        if not isinstance(next_cursor, str) or next_cursor == cursor:
            next_cursor = None
        return LogPage(events=events, next_cursor=next_cursor)


def _epoch_milliseconds(value: datetime) -> int:
    return floor(value.timestamp() * 1_000)


def _parse_events(
    raw_events: object,
    *,
    task_id: str | None = None,
    stream_task_ids: Mapping[str, str | None] | None = None,
    identity_namespace: str | None = None,
) -> list[LogEvent]:
    if not isinstance(raw_events, list):
        raise LogProviderError("CloudWatch returned an invalid events payload")

    events: list[LogEvent] = []
    for index, raw_event in enumerate(cast(list[object], raw_events)):
        if not isinstance(raw_event, Mapping):
            raise LogProviderError("CloudWatch returned an invalid log event")
        event = cast(Mapping[str, object], raw_event)
        timestamp = event.get("timestamp")
        message = event.get("message")
        if not isinstance(timestamp, int) or not isinstance(message, str):
            raise LogProviderError("CloudWatch returned an invalid log event")

        ingestion_timestamp = event.get("ingestionTime")
        ingestion_time = (
            datetime.fromtimestamp(ingestion_timestamp / 1_000, timezone.utc)
            if isinstance(ingestion_timestamp, int)
            else None
        )
        stream_name = event.get("logStreamName")
        resolved_task_id = task_id
        if resolved_task_id is None and isinstance(stream_name, str) and stream_task_ids is not None:
            resolved_task_id = stream_task_ids.get(stream_name)

        event_id = event.get("eventId")
        if not isinstance(event_id, str):
            identity = f"{identity_namespace}:{stream_name}:{timestamp}:{ingestion_timestamp}:{index}:{message}"
            event_id = hashlib.sha256(identity.encode()).hexdigest()

        events.append(
            LogEvent(
                timestamp=datetime.fromtimestamp(timestamp / 1_000, timezone.utc),
                message=message,
                task_id=resolved_task_id,
                ingestion_time=ingestion_time,
                event_id=event_id,
            )
        )
    return events


def _stream_task_ids(reference: RunLogReference) -> dict[str, str | None]:
    stream_task_ids: dict[str, str | None] = {}
    for task in reference.tasks:
        stream_name = task_log_stream_name(task.task_id, task.started_at)
        if stream_name in stream_task_ids and stream_task_ids[stream_name] != task.task_id:
            stream_task_ids[stream_name] = None
        else:
            stream_task_ids[stream_name] = task.task_id
    return stream_task_ids


def _event_sort_key(event: LogEvent) -> tuple[datetime, datetime, str]:
    return (event.timestamp, event.ingestion_time or event.timestamp, event.event_id or "")
