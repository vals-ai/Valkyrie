import re
import time
from collections.abc import Callable
from functools import wraps
from typing import ParamSpec, TypeVar
from urllib.parse import quote

import logfire
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.exceptions import CloudWatchError

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
            except (ClientError, BotoCoreError) as e:
                raise CloudWatchError(f"{message}: {e}") from e

        return wrapper

    return decorator


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
        client.put_log_events(  # pyright: ignore[reportUnknownMemberType]
            logGroupName=log_group_name,
            logStreamName=stream_name,
            logEvents=[{"timestamp": int(time.time() * 1000), "message": message}],
        )
    except (ClientError, BotoCoreError) as e:
        raise CloudWatchError(f"Failed to put log event: {e}") from e
