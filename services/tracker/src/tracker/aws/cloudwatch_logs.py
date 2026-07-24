import base64
import re
import time
from binascii import Error as Base64Error
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from typing import Literal, ParamSpec, TypeAlias, TypeVar, assert_never
from urllib.parse import quote, unquote

import logfire
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.exceptions import CloudWatchError

_created_streams: set[str] = set()
_MAX_LOG_EVENT_BYTES = 1_048_576 - 26
_P = ParamSpec("_P")
_R = TypeVar("_R")


@dataclass(frozen=True)
class TaskLogAttempt:
    attempt_id: str
    started_at: datetime
    creation_time_ms: int
    first_event_time_ms: int | None
    last_event_time_ms: int | None
    last_ingestion_time_ms: int | None


@dataclass(frozen=True)
class TaskLogAttemptsPage:
    attempts: list[TaskLogAttempt]
    next_cursor: str | None


@dataclass(frozen=True)
class TaskLogEvent:
    timestamp_ms: int
    ingestion_time_ms: int
    message: str


@dataclass(frozen=True)
class TaskLogEventsPage:
    events: list[TaskLogEvent]
    older_cursor: str | None
    newer_cursor: str | None


@dataclass(frozen=True)
class RunLogEvent:
    event_id: str
    task_id: str
    attempt_id: str
    timestamp_ms: int
    ingestion_time_ms: int
    message: str


@dataclass(frozen=True)
class RunLogEventsPage:
    events: list[RunLogEvent]
    next_cursor: str
    at_tail: bool


@dataclass(frozen=True)
class _RunLogTokenCursor:
    token: str
    next_timestamp_ms: int


@dataclass(frozen=True)
class _RunLogTimeCursor:
    next_timestamp_ms: int


_RunLogCursor: TypeAlias = _RunLogTokenCursor | _RunLogTimeCursor


def _sanitize_log_stream_name(task_id: str) -> str:
    """Encode a task ID without changing common CloudWatch-safe IDs."""
    return quote(task_id, safe="/-_.~")


def _log_event_chunks(message: str) -> Iterator[str]:
    encoded = message.encode()
    start = 0
    while start < len(encoded):
        end = min(start + _MAX_LOG_EVENT_BYTES, len(encoded))
        while end < len(encoded) and encoded[end] & 0b1100_0000 == 0b1000_0000:
            end -= 1
        yield encoded[start:end].decode()
        start = end


def task_log_attempt_id(started_at: datetime) -> str:
    utc_started_at = (
        started_at.replace(tzinfo=timezone.utc) if started_at.tzinfo is None else started_at.astimezone(timezone.utc)
    )
    return f"{int(utc_started_at.timestamp() * 1_000_000):x}"


def _task_log_stream_prefix(task_id: str) -> str:
    return f"{_sanitize_log_stream_name(task_id)}_"


def _task_log_stream_name(task_id: str, attempt_id: str) -> str:
    return f"{_task_log_stream_prefix(task_id)}{attempt_id}"


def _parse_task_log_stream_name(stream_name: str) -> tuple[str, str] | None:
    encoded_task_id, separator, attempt_id = stream_name.rpartition("_")
    if not separator or not encoded_task_id or not re.fullmatch(r"[0-9a-f]+", attempt_id):
        return None

    task_id = unquote(encoded_task_id)
    if _task_log_stream_name(task_id, attempt_id) != stream_name:
        return None
    return task_id, attempt_id


def _encode_run_log_cursor(cursor: _RunLogCursor) -> str:
    match cursor:
        case _RunLogTokenCursor(token=token, next_timestamp_ms=next_timestamp_ms):
            encoded_token = base64.urlsafe_b64encode(token.encode()).decode().rstrip("=")
            return f"v1t.{encoded_token}.{next_timestamp_ms:x}"
        case _RunLogTimeCursor(next_timestamp_ms=next_timestamp_ms):
            return f"v1s.{next_timestamp_ms:x}"
        case _:
            assert_never(cursor)


def _decode_run_log_cursor(cursor: str | None) -> _RunLogCursor:
    if cursor is None:
        return _RunLogTimeCursor(next_timestamp_ms=0)

    match cursor.split("."):
        case ["v1t", encoded_token, next_timestamp_hex]:
            padding = "=" * (-len(encoded_token) % 4)
            try:
                token = base64.urlsafe_b64decode(encoded_token + padding).decode()
                next_timestamp_ms = int(next_timestamp_hex, 16)
            except (Base64Error, UnicodeDecodeError, ValueError):
                raise ValueError("Invalid run log cursor") from None
            if not token or next_timestamp_ms < 0:
                raise ValueError("Invalid run log cursor")
            return _RunLogTokenCursor(token=token, next_timestamp_ms=next_timestamp_ms)
        case ["v1s", next_timestamp_hex]:
            try:
                next_timestamp_ms = int(next_timestamp_hex, 16)
            except ValueError:
                raise ValueError("Invalid run log cursor") from None
            if next_timestamp_ms < 0:
                raise ValueError("Invalid run log cursor")
            return _RunLogTimeCursor(next_timestamp_ms=next_timestamp_ms)
        case _:
            raise ValueError("Invalid run log cursor")


def handle_cloudwatch_error(message: str) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    def decorator(func: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(func)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return func(*args, **kwargs)
            except (ClientError, BotoCoreError) as e:
                raise CloudWatchError(f"{message}: {e}") from e

        return wrapper

    return decorator


@handle_cloudwatch_error(message="Failed to list task log attempts")
def list_task_log_attempts(
    benchmark_id: str,
    task_id: str,
    runtime: AWSRuntime,
    *,
    limit: int,
    cursor: str | None,
) -> TaskLogAttemptsPage:
    client = runtime.clients.cloudwatch_logs_client()
    log_group_name = f"{runtime.resources.log_group}/{benchmark_id}"
    stream_prefix = _task_log_stream_prefix(task_id)
    attempts: list[TaskLogAttempt] = []
    next_cursor = cursor

    while len(attempts) < limit:
        request = {
            "logGroupName": log_group_name,
            "logStreamNamePrefix": stream_prefix,
            "orderBy": "LogStreamName",
            "descending": True,
            "limit": limit - len(attempts),
        }
        if next_cursor is not None:
            request["nextToken"] = next_cursor

        try:
            response = client.describe_log_streams(**request)  # pyright: ignore[reportUnknownMemberType]
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                return TaskLogAttemptsPage(attempts=[], next_cursor=None)
            raise
        for stream in response["logStreams"]:
            stream_name: str = stream["logStreamName"]
            attempt_id = stream_name.removeprefix(stream_prefix)
            if stream_name != _task_log_stream_name(task_id, attempt_id) or not re.fullmatch(r"[0-9a-f]+", attempt_id):
                continue
            attempts.append(
                TaskLogAttempt(
                    attempt_id=attempt_id,
                    started_at=datetime.fromtimestamp(int(attempt_id, 16) / 1_000_000, tz=timezone.utc),
                    creation_time_ms=stream["creationTime"],
                    first_event_time_ms=stream.get("firstEventTimestamp"),
                    last_event_time_ms=stream.get("lastEventTimestamp"),
                    last_ingestion_time_ms=stream.get("lastIngestionTime"),
                )
            )

        next_cursor = response.get("nextToken")
        if next_cursor is None:
            break

    attempts.sort(key=lambda attempt: int(attempt.attempt_id, 16), reverse=True)
    return TaskLogAttemptsPage(attempts=attempts, next_cursor=next_cursor)


@handle_cloudwatch_error(message="Failed to fetch task log events")
def get_task_log_events(
    benchmark_id: str,
    task_id: str,
    attempt_id: str,
    runtime: AWSRuntime,
    *,
    direction: Literal["forward", "backward"],
    limit: int,
    cursor: str | None,
) -> TaskLogEventsPage | None:
    client = runtime.clients.cloudwatch_logs_client()
    request = {
        "logGroupName": f"{runtime.resources.log_group}/{benchmark_id}",
        "logStreamName": _task_log_stream_name(task_id, attempt_id),
        "limit": limit,
        "startFromHead": direction == "forward",
    }
    if cursor is not None:
        request["nextToken"] = cursor

    try:
        response = client.get_log_events(**request)  # pyright: ignore[reportUnknownMemberType]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return None
        raise
    return TaskLogEventsPage(
        events=[
            TaskLogEvent(
                timestamp_ms=event["timestamp"],
                ingestion_time_ms=event["ingestionTime"],
                message=event["message"],
            )
            for event in response["events"]
        ],
        older_cursor=response["nextBackwardToken"],
        newer_cursor=response["nextForwardToken"],
    )


@handle_cloudwatch_error(message="Failed to fetch run log events")
def get_run_log_events(
    benchmark_id: str,
    runtime: AWSRuntime,
    *,
    limit: int,
    cursor: str | None,
) -> RunLogEventsPage:
    client = runtime.clients.cloudwatch_logs_client()
    parsed_cursor = _decode_run_log_cursor(cursor)
    request = {
        "logGroupName": f"{runtime.resources.log_group}/{benchmark_id}",
        "limit": limit,
    }
    match parsed_cursor:
        case _RunLogTokenCursor(token=token):
            request["nextToken"] = token
        case _RunLogTimeCursor(next_timestamp_ms=next_timestamp_ms):
            request["startTime"] = next_timestamp_ms
        case _:
            assert_never(parsed_cursor)

    try:
        response = client.filter_log_events(**request)  # pyright: ignore[reportUnknownMemberType]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return RunLogEventsPage(
                events=[],
                next_cursor=_encode_run_log_cursor(parsed_cursor),
                at_tail=True,
            )
        raise

    raw_events = response["events"]
    next_timestamp_ms = max(
        parsed_cursor.next_timestamp_ms,
        max((event["timestamp"] + 1 for event in raw_events), default=0),
    )
    events: list[RunLogEvent] = []
    for event in raw_events:
        parsed_stream = _parse_task_log_stream_name(event["logStreamName"])
        if parsed_stream is None:
            continue
        task_id, attempt_id = parsed_stream
        events.append(
            RunLogEvent(
                event_id=event["eventId"],
                task_id=task_id,
                attempt_id=attempt_id,
                timestamp_ms=event["timestamp"],
                ingestion_time_ms=event["ingestionTime"],
                message=event["message"],
            )
        )

    next_token = response.get("nextToken")
    next_cursor = (
        _RunLogTokenCursor(token=next_token, next_timestamp_ms=next_timestamp_ms)
        if next_token is not None
        else _RunLogTimeCursor(next_timestamp_ms=next_timestamp_ms)
    )
    return RunLogEventsPage(
        events=events,
        next_cursor=_encode_run_log_cursor(next_cursor),
        at_tail=next_token is None,
    )


def get_benchmark_log_url(benchmark_id: str, resources: AWSResources, task_id: str | None = None) -> str:
    """
    Get the CloudWatch console URL for a benchmark or specific task.

    Args:
        benchmark_id: The benchmark identifier
        resources: AWS resource locations
        task_id: Optional task identifier for task-specific logs

    Returns:
        CloudWatch console URL
    """
    safe_region = quote(resources.region, safe="-")
    safe_log_group = quote(resources.log_group, safe="-_")
    safe_benchmark_id = quote(benchmark_id, safe="-_")
    base = f"https://{safe_region}.console.aws.amazon.com/cloudwatch/home?region={safe_region}"
    encoded_log_group = f"{safe_log_group}$252F{safe_benchmark_id}"
    if task_id:
        safe_task_id = quote(_sanitize_log_stream_name(task_id), safe="-_.")
        return f"{base}#logsV2:log-groups/log-group/{encoded_log_group}/log-events/{safe_task_id}"

    return f"{base}#logsV2:log-groups/log-group/{encoded_log_group}"


@handle_cloudwatch_error(message="Failed to create log group")
@logfire.instrument("create_log_group", extract_args=("benchmark_id",))
def create_benchmark_log_group(benchmark_id: str, runtime: AWSRuntime) -> str:
    """
    Create a log group for a benchmark.

    Args:
        benchmark_id: The benchmark identifier
        runtime: AWS resources and clients for the operation

    Returns:
        The log group name
    """
    client = runtime.clients.cloudwatch_logs_client()
    log_group_name: str = f"{runtime.resources.log_group}/{benchmark_id}"

    try:
        client.create_log_group(logGroupName=log_group_name)  # pyright: ignore[reportUnknownMemberType]
        client.put_retention_policy(  # pyright: ignore[reportUnknownMemberType]
            logGroupName=log_group_name,
            retentionInDays=runtime.resources.log_retention_days,
        )
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
            raise

    return log_group_name


@handle_cloudwatch_error(message="Failed to create cloudwatch stream")
def write_benchmark_log_event(stream_key: str, message: str, runtime: AWSRuntime) -> None:
    """
    Stream a log message to CloudWatch.

    Creates the log stream if it doesn't exist.

    Args:
        stream_key: The stream key (benchmark_id:task_id)
        message: The log message
        runtime: AWS resources and clients for the operation
    """
    if not message.strip():
        return

    benchmark_id, task_id = stream_key.split(":", 1)

    if not benchmark_id or not task_id:
        raise CloudWatchError(f"Invalid stream key '{stream_key}', expected format 'benchmark_id:task_id'")

    client = runtime.clients.cloudwatch_logs_client()
    log_group_name = f"{runtime.resources.log_group}/{benchmark_id}"
    stream_name = _sanitize_log_stream_name(task_id)

    if stream_key not in _created_streams:
        try:
            client.create_log_stream(logGroupName=log_group_name, logStreamName=stream_name)  # pyright: ignore[reportUnknownMemberType]
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
                raise
        except BotoCoreError as e:
            raise CloudWatchError(f"Failed to create log stream '{stream_name}': {e}") from e
        _created_streams.add(stream_key)

    try:
        for chunk in _log_event_chunks(message):
            client.put_log_events(  # pyright: ignore[reportUnknownMemberType]
                logGroupName=log_group_name,
                logStreamName=stream_name,
                logEvents=[{"timestamp": int(time.time() * 1000), "message": chunk}],
            )
    except (ClientError, BotoCoreError) as e:
        raise CloudWatchError(f"Failed to put log event: {e}") from e
