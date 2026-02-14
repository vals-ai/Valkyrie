import time

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from tracker.exceptions import CloudWatchError

_client = boto3.client("logs", config=Config(max_pool_connections=75))  # pyright: ignore[reportUnknownMemberType] # type: ignore[reportUnknownReturnType]
_created_streams: set[str] = set()

ROOT_LOG_GROUP = "benchmarks"


def get_cloudwatch_url(benchmark_id: str, task_id: str | None = None) -> str:
    """
    Get the CloudWatch console URL for a benchmark or specific task.

    Args:
        benchmark_id: The benchmark identifier
        task_id: Optional task identifier for task-specific logs

    Returns:
        CloudWatch console URL
    """
    region = _client.meta.region_name  # pyright: ignore
    base = f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
    log_group = f"benchmarks$252F{benchmark_id}"
    if task_id:
        return f"{base}#logsV2:log-groups/log-group/{log_group}/log-events/{task_id}"
    return f"{base}#logsV2:log-groups/log-group/{log_group}"


def create_benchmark_group(benchmark_id: str) -> str:
    """
    Create a log group for a benchmark with 1-day retention.

    Args:
        benchmark_id: The benchmark identifier

    Returns:
        The log group name
    """
    log_group_name: str = f"{ROOT_LOG_GROUP}/{benchmark_id}"

    try:
        _client.create_log_group(logGroupName=log_group_name)  # pyright: ignore[reportUnknownMemberType]
        _client.put_retention_policy(logGroupName=log_group_name, retentionInDays=365)  # pyright: ignore[reportUnknownMemberType]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
            raise CloudWatchError(f"Failed to create log group '{log_group_name}': {e}") from e
    except BotoCoreError as e:
        raise CloudWatchError(f"Failed to create log group '{log_group_name}': {e}") from e

    return log_group_name


def reset_cloudwatch_stream(stream_key: str) -> None:
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

    try:
        _client.delete_log_stream(logGroupName=log_group_name, logStreamName=task_id)  # pyright: ignore[reportUnknownMemberType]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise CloudWatchError(f"Failed to delete log stream '{task_id}': {e}") from e
    except BotoCoreError as e:
        raise CloudWatchError(f"Failed to delete log stream '{task_id}': {e}") from e

    _created_streams.discard(stream_key)


def cloudwatch_stream(stream_key: str, message: str) -> None:
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

    if stream_key not in _created_streams:
        try:
            _client.create_log_stream(logGroupName=f"{ROOT_LOG_GROUP}/{benchmark_id}", logStreamName=task_id)  # pyright: ignore[reportUnknownMemberType]
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
                raise CloudWatchError(f"Failed to create log stream '{task_id}': {e}") from e
        except BotoCoreError as e:
            raise CloudWatchError(f"Failed to create log stream '{task_id}': {e}") from e
        _created_streams.add(stream_key)

    try:
        _client.put_log_events(  # pyright: ignore[reportUnknownMemberType]
            logGroupName=f"{ROOT_LOG_GROUP}/{benchmark_id}",
            logStreamName=task_id,
            logEvents=[{"timestamp": int(time.time() * 1000), "message": message}],
        )
    except (ClientError, BotoCoreError) as e:
        raise CloudWatchError(f"Failed to put log event: {e}") from e
