import asyncio
import concurrent.futures
import time
from collections import defaultdict
from functools import lru_cache, wraps
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from tracker.exceptions import CloudWatchError
from tracker.logging import get_logger

if TYPE_CHECKING:
    from tracker.types import AWSCredentials

logger = get_logger(__name__)

_created_streams: set[str] = set()

# Dedicated pool for CloudWatch flushes (4 vCPU / 8 GiB worker).
# put_log_events is ~90% network-wait, so thread count >> CPU count is correct.
# Math: 1000 tasks × 1 flush/s × ~100ms/flush = 100 concurrent threads needed.
# 96 workers gives ~960 flushes/s capacity with headroom; ~100 MB stack overhead.
_cw_executor: concurrent.futures.ThreadPoolExecutor = concurrent.futures.ThreadPoolExecutor(
    max_workers=96, thread_name_prefix="cw-flush"
)


@lru_cache(maxsize=32)
def _cloudwatch_client(aws: "AWSCredentials") -> Any:
    """Cloudwatch client cached to share instances."""
    return boto3.client(  # pyright: ignore[reportUnknownMemberType]
        "logs",
        aws_access_key_id=aws.aws_access_key_id,
        aws_secret_access_key=aws.aws_secret_access_key,
        region_name=aws.aws_default_region,
        config=Config(max_pool_connections=200),
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


def get_cloudwatch_url(benchmark_id: str, region: str, log_group: str, task_id: str | None = None) -> str:
    """
    Get the CloudWatch console URL for a benchmark or specific task.

    Args:
        benchmark_id: The benchmark identifier
        region: The AWS region
        log_group: The root log group name
        task_id: Optional task identifier for task-specific logs

    Returns:
        CloudWatch console URL
    """
    base = f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
    encoded_log_group = f"{log_group}$252F{benchmark_id}"
    if task_id:
        return f"{base}#logsV2:log-groups/log-group/{encoded_log_group}/log-events/{task_id}"

    return f"{base}#logsV2:log-groups/log-group/{encoded_log_group}"


@handle_cloudwatch_error(message="Failed to create log group")
def create_benchmark_group(benchmark_id: str, aws: "AWSCredentials", log_group: str, log_retention_policy: int) -> str:
    """
    Create a log group for a benchmark.

    Args:
        benchmark_id: The benchmark identifier
        aws: AWS credentials for CloudWatch client
        log_group: The root log group name
        log_retention_policy: Number of days to retain logs

    Returns:
        The log group name
    """
    client = _cloudwatch_client(aws)
    log_group_name: str = f"{log_group}/{benchmark_id}"

    try:
        client.create_log_group(logGroupName=log_group_name)  # pyright: ignore[reportUnknownMemberType]
        client.put_retention_policy(logGroupName=log_group_name, retentionInDays=log_retention_policy)  # pyright: ignore[reportUnknownMemberType]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
            raise

    return log_group_name


@handle_cloudwatch_error(message="Failed to delete log stream")
def reset_cloudwatch_stream(stream_key: str, aws: "AWSCredentials", log_group: str) -> None:
    """
    Delete and recreate a CloudWatch log stream to reset it.

    Used when restarting a task to clear old logs from previous runs.

    Args:
        stream_key: The stream key (benchmark_id:task_id)
        aws: AWS credentials for CloudWatch client
        log_group: The root log group name
    """
    benchmark_id, task_id = stream_key.split(":")

    if not benchmark_id or not task_id:
        raise CloudWatchError(f"Invalid stream key '{stream_key}', expected format 'benchmark_id:task_id'")

    client = _cloudwatch_client(aws)
    log_group_name = f"{log_group}/{benchmark_id}"

    try:
        client.delete_log_stream(logGroupName=log_group_name, logStreamName=task_id)  # pyright: ignore[reportUnknownMemberType]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            raise

    _created_streams.discard(stream_key)


@handle_cloudwatch_error(message="Failed to create cloudwatch stream")
def cloudwatch_stream(stream_key: str, message: str, aws: "AWSCredentials", log_group: str) -> None:
    """
    Stream a log message to CloudWatch.

    Creates the log stream if it doesn't exist.

    Args:
        stream_key: The stream key (benchmark_id:task_id)
        message: The log message
        aws: AWS credentials for CloudWatch client
        log_group: The root log group name
    """
    if not message.strip():
        return

    benchmark_id, task_id = stream_key.split(":")

    if not benchmark_id or not task_id:
        raise CloudWatchError(f"Invalid stream key '{stream_key}', expected format 'benchmark_id:task_id'")

    client = _cloudwatch_client(aws)
    log_group_name = f"{log_group}/{benchmark_id}"

    if stream_key not in _created_streams:
        try:
            client.create_log_stream(logGroupName=log_group_name, logStreamName=task_id)  # pyright: ignore[reportUnknownMemberType]
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ResourceAlreadyExistsException":
                raise
        except BotoCoreError as e:
            raise CloudWatchError(f"Failed to create log stream '{task_id}': {e}") from e
        _created_streams.add(stream_key)

    try:
        client.put_log_events(  # pyright: ignore[reportUnknownMemberType]
            logGroupName=log_group_name,
            logStreamName=task_id,
            logEvents=[{"timestamp": int(time.time() * 1000), "message": message}],
        )
    except (ClientError, BotoCoreError) as e:
        raise CloudWatchError(f"Failed to put log event: {e}") from e


class LogDispatcher:
    """Single CloudWatch dispatcher for all streams in a benchmark run."""

    def __init__(self, aws: "AWSCredentials", log_group: str) -> None:
        self._aws = aws
        self._log_group = log_group
        self._queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=500_000)
        self._stop = asyncio.Event()
        self._consumer: asyncio.Task[None] | None = None
        self._pending: set[asyncio.Task[None]] = set()
        self._dropped = 0

    def log(self, stream_key: str, data: str) -> None:
        """Non-blocking enqueue. Drops silently on overflow."""
        if not data:
            return
        try:
            self._queue.put_nowait((stream_key, data))
        except asyncio.QueueFull:
            self._dropped += 1

    async def async_log(self, stream_key: str, data: str) -> None:
        """Awaitable enqueue with backpressure."""
        if not data:
            return
        await self._queue.put((stream_key, data))

    def start(self) -> None:
        self._consumer = asyncio.create_task(self._run(), name="cw-dispatcher")

    async def stop(self) -> None:
        assert self._consumer is not None
        self._stop.set()
        try:
            await asyncio.wait_for(asyncio.shield(self._consumer), timeout=15.0)
        except asyncio.TimeoutError:
            self._consumer.cancel()
            try:
                await self._consumer
            except (asyncio.CancelledError, Exception):
                pass
        if self._pending:
            await asyncio.gather(*list(self._pending), return_exceptions=True)
        if self._dropped:
            logger.warning(f"LogDispatcher dropped {self._dropped} messages (queue overflow)")

    async def _run(self) -> None:
        batches: defaultdict[str, list[str]] = defaultdict(list)
        chars: defaultdict[str, int] = defaultdict(int)
        last_flush = time.monotonic()

        while True:
            # Fast-path: drain all queued items without scheduler overhead.
            while True:
                try:
                    stream_key, msg = self._queue.get_nowait()
                    batches[stream_key].append(msg)
                    chars[stream_key] += len(msg)
                except asyncio.QueueEmpty:
                    break

            now = time.monotonic()
            flush_interval = now - last_flush >= 1.0

            ready = [k for k in batches if len(batches[k]) >= 256 or chars[k] >= 200_000 or flush_interval]
            for key in ready:
                self._dispatch(key, batches.pop(key))
                chars.pop(key)

            if flush_interval:
                last_flush = now

            if self._stop.is_set() and self._queue.empty():
                for key, batch in batches.items():
                    self._dispatch(key, batch)
                return

            # Slow-path: block until next message or flush deadline.
            remaining = max(0.0, last_flush + 1.0 - time.monotonic())
            try:
                async with asyncio.timeout(remaining):
                    stream_key, msg = await self._queue.get()
                batches[stream_key].append(msg)
                chars[stream_key] += len(msg)
            except TimeoutError:
                pass

    def _dispatch(self, stream_key: str, batch: list[str]) -> None:
        t = asyncio.create_task(self._flush(stream_key, batch))
        self._pending.add(t)
        t.add_done_callback(self._pending.discard)

    async def _flush(self, stream_key: str, batch: list[str]) -> None:
        try:
            await asyncio.get_running_loop().run_in_executor(
                _cw_executor, cloudwatch_stream, stream_key, "".join(batch), self._aws, self._log_group
            )
        except CloudWatchError as e:
            logger.warning(f"flush failed {stream_key}: {e}")
