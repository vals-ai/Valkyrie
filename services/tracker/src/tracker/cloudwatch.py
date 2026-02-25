import time
from functools import wraps
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from tracker.exceptions import CloudWatchError

if TYPE_CHECKING:
    from tracker.types import HarnessConfig

_created_streams: set[str] = set()


def _cloudwatch_client(harness_config: "HarnessConfig") -> Any:
    """Create a CloudWatch Logs client from harness config credentials."""
    return boto3.client(  # pyright: ignore[reportUnknownMemberType]
        "logs",
        aws_access_key_id=harness_config.aws_access_key_id,
        aws_secret_access_key=harness_config.aws_secret_access_key,
        region_name=harness_config.aws_default_region,
        config=Config(max_pool_connections=75),
    )


def handle_cloudwatch_error(message: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (ClientError, BotoCoreError) as e:
                raise CloudWatchError(f"{message}: {e}") from e

        return wrapper

    return decorator


def get_cloudwatch_url(benchmark_id: str, harness_config: "HarnessConfig", task_id: str | None = None) -> str:
    """
    Get the CloudWatch console URL for a benchmark or specific task.

    Args:
        benchmark_id: The benchmark identifier
        harness_config: Harness config providing the region and log group
        task_id: Optional task identifier for task-specific logs

    Returns:
        CloudWatch console URL
    """
    region = harness_config.aws_default_region
    root_log_group = harness_config.root_log_group
    base = f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
    encoded_log_group = f"{root_log_group}$252F{benchmark_id}"
    if task_id:
        return f"{base}#logsV2:log-groups/log-group/{encoded_log_group}/log-events/{task_id}"

    return f"{base}#logsV2:log-groups/log-group/{encoded_log_group}"


@handle_cloudwatch_error(message="Failed to create log group")
def create_benchmark_group(benchmark_id: str, harness_config: "HarnessConfig") -> str:
    """
    Create a log group for a benchmark.

    Args:
        benchmark_id: The benchmark identifier
        harness_config: Harness config providing credentials, log group, and retention policy

    Returns:
        The log group name
    """
    client = _cloudwatch_client(harness_config)
    log_group_name: str = f"{harness_config.root_log_group}/{benchmark_id}"

    try:
        client.create_log_group(logGroupName=log_group_name)  # pyright: ignore[reportUnknownMemberType]
        client.put_retention_policy(logGroupName=log_group_name, retentionInDays=harness_config.log_retention_policy)  # pyright: ignore[reportUnknownMemberType]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
            raise

    return log_group_name


@handle_cloudwatch_error(message="Failed to delete log stream")
def reset_cloudwatch_stream(stream_key: str, harness_config: "HarnessConfig") -> None:
    """
    Delete and recreate a CloudWatch log stream to reset it.

    Used when restarting a task to clear old logs from previous runs.

    Args:
        stream_key: The stream key (benchmark_id:task_id)
        harness_config: Harness config providing credentials and log group
    """
    benchmark_id, task_id = stream_key.split(":")

    if not benchmark_id or not task_id:
        raise CloudWatchError(f"Invalid stream key '{stream_key}', expected format 'benchmark_id:task_id'")

    client = _cloudwatch_client(harness_config)
    log_group_name = f"{harness_config.root_log_group}/{benchmark_id}"

    try:
        client.delete_log_stream(logGroupName=log_group_name, logStreamName=task_id)  # pyright: ignore[reportUnknownMemberType]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise

    _created_streams.discard(stream_key)


@handle_cloudwatch_error(message="Failed to create cloudwatch stream")
def cloudwatch_stream(stream_key: str, message: str, harness_config: "HarnessConfig") -> None:
    """
    Stream a log message to CloudWatch.

    Creates the log stream if it doesn't exist.

    Args:
        stream_key: The stream key (benchmark_id:task_id)
        message: The log message
        harness_config: Harness config providing credentials and log group
    """
    if not message.strip():
        return

    benchmark_id, task_id = stream_key.split(":")

    if not benchmark_id or not task_id:
        raise CloudWatchError(f"Invalid stream key '{stream_key}', expected format 'benchmark_id:task_id'")

    client = _cloudwatch_client(harness_config)
    log_group = f"{harness_config.root_log_group}/{benchmark_id}"

    if stream_key not in _created_streams:
        try:
            client.create_log_stream(logGroupName=log_group, logStreamName=task_id)  # pyright: ignore[reportUnknownMemberType]
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
                raise
        except BotoCoreError as e:
            raise CloudWatchError(f"Failed to create log stream '{task_id}': {e}") from e
        _created_streams.add(stream_key)

    try:
        client.put_log_events(  # pyright: ignore[reportUnknownMemberType]
            logGroupName=log_group,
            logStreamName=task_id,
            logEvents=[{"timestamp": int(time.time() * 1000), "message": message}],
        )
    except (ClientError, BotoCoreError) as e:
        raise CloudWatchError(f"Failed to put log event: {e}") from e
