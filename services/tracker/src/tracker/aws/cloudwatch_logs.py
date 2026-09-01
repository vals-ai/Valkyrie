import re
import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar
from urllib.parse import quote

import logfire
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws.clients import AWSClientProvider
from tracker.aws.runtime import AWSResources
from tracker.exceptions import CloudWatchError
from tracker.runtime.logs import BenchmarkLogLocations, BenchmarkLogSink

_created_streams: set[str] = set()
_P = ParamSpec("_P")
_R = TypeVar("_R")


def _sanitize_log_stream_name(task_id: str) -> str:
    """Make a task_id safe to use as a CloudWatch logStreamName.

    AWS requires log stream names to match the regex ``[^:*]*`` (no ``:`` or
    ``*``). Some task ids carry these characters (e.g. model-suffixed ids like
    ``provider/model:fast``), which makes ``CreateLogStream`` raise
    ``InvalidParameterException`` and silently drops the run's logs. Replace the
    forbidden characters so logging degrades gracefully instead of failing.
    """
    return re.sub(r"[:*]", "_", task_id)


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
        log_group_name = f"{self._log_group}/{benchmark_id}"
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
        log_group_name = f"{self._log_group}/{benchmark_id}"
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
