import time
from functools import wraps
from typing import Any, Callable, Coroutine

import aiobotocore.session
from aiobotocore.config import AioConfig
from botocore.exceptions import BotoCoreError, ClientError

from tracker.exceptions import CloudWatchError

_session = aiobotocore.session.get_session()  # pyright: ignore[reportUnknownMemberType]
_client_config = AioConfig(max_pool_connections=75)
_created_streams: set[str] = set()

ROOT_LOG_GROUP = "benchmarks"


def handle_cloudwatch_error(message: str) -> Callable[..., Callable[..., Coroutine[Any, Any, Any]]]:
    def decorator(func: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., Coroutine[Any, Any, Any]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except (ClientError, BotoCoreError) as e:
                raise CloudWatchError(f"{message}: {e}") from e

        return wrapper

    return decorator


def get_cloudwatch_url(benchmark_id: str, task_id: str | None = None) -> str:
    """
    Get the CloudWatch console URL for a benchmark or specific task.

    Args:
        benchmark_id: The benchmark identifier
        task_id: Optional task identifier for task-specific logs

    Returns:
        CloudWatch console URL
    """
    region = _session.get_config_variable("region")  # pyright: ignore[reportUnknownMemberType]
    base = f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
    log_group = f"benchmarks$252F{benchmark_id}"
    if task_id:
        return f"{base}#logsV2:log-groups/log-group/{log_group}/log-events/{task_id}"

    return f"{base}#logsV2:log-groups/log-group/{log_group}"


@handle_cloudwatch_error(message="Failed to create log group")
async def create_benchmark_group(benchmark_id: str) -> str:
    """
    Create a log group for a benchmark with 1-day retention.

    Args:
        benchmark_id: The benchmark identifier

    Returns:
        The log group name
    """
    log_group_name: str = f"{ROOT_LOG_GROUP}/{benchmark_id}"

    async with _session.create_client("logs", config=_client_config) as client:  # pyright: ignore[reportUnknownMemberType]
        try:
            await client.create_log_group(logGroupName=log_group_name)  # pyright: ignore
            await client.put_retention_policy(logGroupName=log_group_name, retentionInDays=365)  # pyright: ignore
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
                raise

    return log_group_name


@handle_cloudwatch_error(message="Failed to delete log stream")
async def reset_cloudwatch_stream(stream_key: str) -> None:
    """
    Delete and recreate a CloudWatch log stream to reset it.

    Used when restarting a task to clear old logs from previous runs.

    Args:
        stream_key: The stream key (benchmark_id:task_id)
    """
    benchmark_id, task_id = stream_key.split(":")

    if not benchmark_id or not task_id:
        raise CloudWatchError(f"Invalid stream key '{stream_key}', expected format 'benchmark_id:task_id'")

    log_group_name = f"{ROOT_LOG_GROUP}/{benchmark_id}"

    async with _session.create_client("logs", config=_client_config) as client:  # pyright: ignore[reportUnknownMemberType]
        try:
            await client.delete_log_stream(logGroupName=log_group_name, logStreamName=task_id)  # pyright: ignore
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
                raise

    _created_streams.discard(stream_key)


@handle_cloudwatch_error(message="Failed to create cloudwatch stream")
async def cloudwatch_stream(stream_key: str, message: str) -> None:
    """
    Stream a log message to CloudWatch.

    Creates the log stream if it doesn't exist.

    Args:
        stream_key: The stream key (benchmark_id:task_id)
        message: The log message
    """
    if not message.strip():
        return

    benchmark_id, task_id = stream_key.split(":")

    if not benchmark_id or not task_id:
        raise CloudWatchError(f"Invalid stream key '{stream_key}', expected format 'benchmark_id:task_id'")

    async with _session.create_client("logs", config=_client_config) as client:  # pyright: ignore[reportUnknownMemberType]
        if stream_key not in _created_streams:
            try:
                await client.create_log_stream(logGroupName=f"{ROOT_LOG_GROUP}/{benchmark_id}", logStreamName=task_id)  # pyright: ignore
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
                    raise
            except BotoCoreError as e:
                raise CloudWatchError(f"Failed to create log stream '{task_id}': {e}") from e
            _created_streams.add(stream_key)

        try:
            await client.put_log_events(  # pyright: ignore
                logGroupName=f"{ROOT_LOG_GROUP}/{benchmark_id}",
                logStreamName=task_id,
                logEvents=[{"timestamp": int(time.time() * 1000), "message": message}],
            )
        except (ClientError, BotoCoreError) as e:
            raise CloudWatchError(f"Failed to put log event: {e}") from e
