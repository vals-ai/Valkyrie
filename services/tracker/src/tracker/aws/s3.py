"""S3 upload utilities for the tracker service."""

from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Coroutine, Iterable
from datetime import datetime
from functools import lru_cache, wraps
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import aioboto3
import logfire
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws.credentials import aws_client_kwargs
from tracker.exceptions import S3Error
from tracker.logging import get_logger

logger = get_logger(__name__)

if TYPE_CHECKING:
    from tracker.types import AWSConfig

_P = ParamSpec("_P")
_R = TypeVar("_R")

S3_AGENTS_PREFIX = "agents"
S3_BENCHMARKS_PREFIX = "benchmarks"

_CLIENT_CONFIG = Config(max_pool_connections=200)


@lru_cache(maxsize=32)
def _s3_session(aws: "AWSConfig") -> aioboto3.Session:
    """aioboto3 session cached per credential set."""
    return aioboto3.Session(**aws_client_kwargs(aws))


def _s3_client(aws: "AWSConfig") -> Any:
    """Open an async S3 client for the given credentials (use as `async with`)."""
    return _s3_session(aws).client("s3", config=_CLIENT_CONFIG)  # pyright: ignore[reportUnknownMemberType]


def scope_s3_key(s3_prefix: str, key: str) -> str:
    return f"{s3_prefix}/{key}" if s3_prefix else key


def get_contract_s3_key(contract_name: str, s3_prefix: str = "") -> str:
    """Get the S3 key for an agent zip file."""
    return scope_s3_key(s3_prefix, f"{S3_AGENTS_PREFIX}/{contract_name}.zip")


def get_benchmark_s3_prefix(benchmark_id: str, s3_prefix: str = "") -> str:
    return scope_s3_key(s3_prefix, f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/")


def get_benchmark_contract_s3_key(benchmark_id: str, contract_name: str, s3_prefix: str = "") -> str:
    """Get the S3 key for an agent zip copied into a benchmark's folder."""
    return f"{get_benchmark_s3_prefix(benchmark_id, s3_prefix)}{contract_name}.zip"


def get_agent_result_s3_key(benchmark_id: str, task_id: str, output_name: str, s3_prefix: str = "") -> str:
    """Get the S3 key for a run output archive."""
    return f"{get_benchmark_s3_prefix(benchmark_id, s3_prefix)}{task_id}/{output_name}"


def handle_s3_error(message: str) -> Callable[[Callable[_P, Awaitable[_R]]], Callable[_P, Coroutine[Any, Any, _R]]]:
    """Wrap AWS errors raised by an async S3 helper as S3Error."""

    def decorator(func: Callable[_P, Awaitable[_R]]) -> Callable[_P, Coroutine[Any, Any, _R]]:
        @wraps(func)
        async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return await func(*args, **kwargs)
            except (ClientError, BotoCoreError) as e:
                raise S3Error(f"{message}: {e}") from e

        return wrapper

    return decorator


@logfire.instrument("upload_to_s3", extract_args=("s3_key", "s3_bucket"))
@handle_s3_error(message="Failed to upload to S3")
async def upload_to_s3(file_content: bytes, s3_key: str, aws: "AWSConfig", s3_bucket: str) -> None:
    """
    Upload file content to S3.

    Args:
        file_content: File content as bytes
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Raises:
        S3Error: If upload fails due to AWS errors or network issues
    """
    async with _s3_client(aws) as client:
        await client.put_object(Bucket=s3_bucket, Key=s3_key, Body=file_content)


@handle_s3_error(message="Failed to download from S3")
async def download_from_s3(s3_key: str, aws: "AWSConfig", s3_bucket: str) -> bytes:
    """
    Download file content from S3.

    Args:
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Returns:
        File content as bytes

    Raises:
        S3Error: If download fails due to AWS errors, network issues, or file not found
    """
    async with _s3_client(aws) as client:
        response = await client.get_object(Bucket=s3_bucket, Key=s3_key)
        async with response["Body"] as stream:
            return await stream.read()


async def _as_async_iter(keys: AsyncIterable[str] | Iterable[str]) -> AsyncIterator[str]:
    """Normalize a sync or async iterable of keys into an async iterator."""
    if isinstance(keys, AsyncIterable):
        async for key in keys:
            yield key
    else:
        for key in keys:
            yield key


async def download_many_from_s3(
    s3_keys: AsyncIterable[str] | Iterable[str], aws: "AWSConfig", s3_bucket: str
) -> AsyncIterator[tuple[str, bytes]]:
    """Download multiple objects over a single shared client (one connection pool).

    Accepts a sync or async iterable of keys, so it can stream lazily from
    list_s3_objects without materializing the full key list. Best-effort: keys
    that fail to download are logged and skipped. Yields ``(s3_key, content)``;
    each object is read fully into memory one at a time, so peak memory is
    bounded by the largest single object rather than the whole set.
    """
    async with _s3_client(aws) as client:
        async for s3_key in _as_async_iter(s3_keys):
            try:
                response = await client.get_object(Bucket=s3_bucket, Key=s3_key)
                async with response["Body"] as stream:
                    yield s3_key, await stream.read()
            except (ClientError, BotoCoreError) as e:
                logger.warning(f"Failed to download {s3_key} from S3: {e}")


@handle_s3_error(message="Failed to delete from S3")
async def delete_from_s3(s3_key: str, aws: "AWSConfig", s3_bucket: str) -> None:
    """
    Delete file from S3.

    Args:
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Raises:
        S3Error: If deletion fails due to AWS errors or network issues
    """
    async with _s3_client(aws) as client:
        await client.delete_object(Bucket=s3_bucket, Key=s3_key)


async def copy_s3_object(source_key: str, dest_key: str, aws: "AWSConfig", s3_bucket: str) -> None:
    """
    Copy an S3 object from source_key to dest_key within the same bucket.

    Raises:
        S3Error: If copy fails due to AWS errors or network issues
    """
    try:
        async with _s3_client(aws) as client:
            await client.copy_object(
                Bucket=s3_bucket,
                CopySource={"Bucket": s3_bucket, "Key": source_key},
                Key=dest_key,
            )
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to copy S3 object from {source_key} to {dest_key}: {e}") from e


async def copy_agent_to_benchmark(
    benchmark_id: str,
    contract_name: str,
    aws: "AWSConfig",
    s3_bucket: str,
    s3_prefix: str = "",
) -> None:
    """
    Freeze the agent for a benchmark run by copying
    agents/<name>.zip -> benchmarks/<benchmark_id>/<name>.zip.

    # NOTE: Skips if it already exists at that location
    """
    source_key = get_contract_s3_key(contract_name, s3_prefix)
    dest_key = get_benchmark_contract_s3_key(benchmark_id, contract_name, s3_prefix)

    if await s3_object_exists(dest_key, aws, s3_bucket):
        return

    await copy_s3_object(source_key, dest_key, aws, s3_bucket)


@handle_s3_error(message="Failed to check S3 object existence")
async def s3_object_exists(s3_key: str, aws: "AWSConfig", s3_bucket: str) -> bool:
    """
    Check if an S3 object exists.

    Args:
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Returns:
        True if the object exists, False otherwise
    """
    async with _s3_client(aws) as client:
        try:
            await client.head_object(Bucket=s3_bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":  # pyright: ignore[reportTypedDictNotRequiredAccess]
                return False

            raise


async def list_s3_objects(prefix: str, aws: "AWSConfig", s3_bucket: str) -> AsyncIterator[str]:
    """
    Yield S3 object keys with the given prefix, a page at a time (no full list held in memory).

    Args:
        prefix: S3 prefix to filter objects
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Yields:
        S3 object keys

    Raises:
        S3Error: If listing fails due to AWS errors or network issues
    """
    try:
        async with _s3_client(aws) as client:
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
                for s3_object in page.get("Contents", []):
                    if "Key" in s3_object:
                        yield s3_object["Key"]
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to list objects from S3: {e}") from e


@handle_s3_error(message="Failed to create presigned URL")
async def create_presigned_url(s3_key: str, aws: "AWSConfig", s3_bucket: str, expiration: int = 86400) -> str:
    """
    Create a presigned URL for an S3 object.

    Args:
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name
        expiration: URL expiration time in seconds (default: 1 day)

    Returns:
        Presigned URL as a string

    Raises:
        S3Error: If presigned URL creation fails
    """
    async with _s3_client(aws) as client:
        presigned_url: str = await client.generate_presigned_url(
            "get_object",
            Params={"Bucket": s3_bucket, "Key": s3_key},
            ExpiresIn=expiration,
        )

    return presigned_url


@handle_s3_error(message="Failed to create presigned upload")
async def create_presigned_upload(
    s3_key: str,
    aws: "AWSConfig",
    s3_bucket: str,
    max_bytes: int,
    expiration: int = 300,
) -> dict[str, Any]:
    """Create a presigned POST that enforces an upload size limit."""
    async with _s3_client(aws) as client:
        upload: dict[str, Any] = await client.generate_presigned_post(
            Bucket=s3_bucket,
            Key=s3_key,
            Conditions=[["content-length-range", 1, max_bytes]],
            ExpiresIn=expiration,
        )

    return upload


@handle_s3_error(message="Failed to inspect S3 object")
async def get_s3_object_size(s3_key: str, aws: "AWSConfig", s3_bucket: str) -> int:
    """Return an S3 object's content length in bytes."""
    async with _s3_client(aws) as client:
        response = await client.head_object(Bucket=s3_bucket, Key=s3_key)
        return int(response["ContentLength"])


def create_console_url(s3_key: str, region: str, s3_bucket: str) -> str:
    """
    Create an AWS console URL for an S3 object.

    Args:
        s3_key: S3 object key (path in bucket)
        region: AWS region
        s3_bucket: S3 bucket name

    Returns:
        AWS console URL as a string
    """
    return f"https://{region}.console.aws.amazon.com/s3/object/{s3_bucket}?region={region}&prefix={s3_key}"


def create_benchmark_url(benchmark_id: str, region: str, s3_bucket: str, s3_prefix: str = "") -> str:
    """
    Create the AWS Console URL for a benchmark's S3 folder.

    Args:
        benchmark_id: Benchmark UUID as a string
        region: AWS region
        s3_bucket: S3 bucket name

    Returns:
        AWS Console URL pointing to the benchmark folder prefix
    """
    prefix = get_benchmark_s3_prefix(benchmark_id, s3_prefix)
    return f"https://{region}.console.aws.amazon.com/s3/buckets/{s3_bucket}?region={region}&prefix={prefix}"


@handle_s3_error(message="Failed to list agents from S3")
async def list_agents(aws: "AWSConfig", s3_bucket: str, s3_prefix: str = "") -> list[tuple[str, datetime | None]]:
    """List zipped agent bundles under the `agents/` prefix.

    Returns (name, last_modified) pairs, one per `agents/<name>.zip`.

    Raises:
        S3Error: If listing fails due to AWS errors or network issues
    """
    agents: list[tuple[str, datetime | None]] = []
    agents_prefix = scope_s3_key(s3_prefix, f"{S3_AGENTS_PREFIX}/")
    async with _s3_client(aws) as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=s3_bucket, Prefix=agents_prefix):
            for s3_object in page.get("Contents", []):
                tail = s3_object["Key"][len(agents_prefix) :]
                if not tail.endswith(".zip"):
                    continue
                agents.append((tail[: -len(".zip")], s3_object.get("LastModified")))

    return agents
