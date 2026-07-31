"""S3 upload utilities for the tracker service."""

from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Coroutine, Iterable
from contextlib import suppress
from datetime import datetime
from functools import lru_cache, wraps
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import aioboto3
import logfire
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from tracker.exceptions import S3Error
from tracker.logging import get_logger

logger = get_logger(__name__)

if TYPE_CHECKING:
    from tracker.types import AWSCredentials

_P = ParamSpec("_P")
_R = TypeVar("_R")

S3_AGENTS_PREFIX = "agents"
S3_BENCHMARKS_PREFIX = "benchmarks"

_CLIENT_CONFIG = Config(max_pool_connections=200, retries={"mode": "standard"})

# S3 multipart uploads require every part except the last to be at least 5 MiB.
_MULTIPART_PART_BYTES = 8 * 1024 * 1024


@lru_cache(maxsize=32)
def _s3_session(aws: "AWSCredentials") -> aioboto3.Session:
    """aioboto3 session cached per credential set."""
    return aioboto3.Session(
        aws_access_key_id=aws.aws_access_key_id,
        aws_secret_access_key=aws.aws_secret_access_key,
        aws_session_token=aws.aws_session_token,
        region_name=aws.aws_default_region,
    )


def s3_client(aws: "AWSCredentials") -> Any:
    """Open an async S3 client for the given credentials (use as `async with`)."""
    return _s3_session(aws).client("s3", config=_CLIENT_CONFIG)  # pyright: ignore[reportUnknownMemberType]


def get_contract_s3_key(contract_name: str) -> str:
    """Get the S3 key for an agent zip file."""
    return f"{S3_AGENTS_PREFIX}/{contract_name}.zip"


def get_benchmark_contract_s3_key(benchmark_id: str, contract_name: str) -> str:
    """Get the S3 key for an agent zip copied into a benchmark's folder."""
    return f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{contract_name}.zip"


def get_agent_result_s3_key(benchmark_id: str, task_id: str, output_name: str) -> str:
    """Get the S3 key for a run output archive."""
    return f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{task_id}/{output_name}"


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
async def upload_to_s3(file_content: bytes, s3_key: str, aws: "AWSCredentials", s3_bucket: str) -> None:
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
    async with s3_client(aws) as client:
        await client.put_object(Bucket=s3_bucket, Key=s3_key, Body=file_content)


@logfire.instrument("upload_stream_to_s3", extract_args=("s3_key", "s3_bucket"))
@handle_s3_error(message="Failed to upload stream to S3")
async def upload_stream_to_s3(
    chunks: AsyncIterable[bytes],
    s3_key: str,
    aws: "AWSCredentials",
    s3_bucket: str,
    should_continue: Callable[[], bool] | None = None,
) -> int:
    """
    Upload a byte stream to S3 via multipart upload, buffering at most one part in memory.

    Args:
        chunks: Async iterable of byte chunks to upload
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name
        should_continue: Optional authority check before each part and completion

    Returns:
        Total number of bytes uploaded

    Raises:
        S3Error: If upload fails due to AWS errors or network issues
    """
    total_bytes = 0
    async with s3_client(aws) as client:
        multipart = await client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key)
        upload_id = multipart["UploadId"]
        try:
            parts: list[dict[str, Any]] = []
            buffer = bytearray()

            async def _upload_part() -> None:
                if should_continue is not None and not should_continue():
                    raise S3Error("S3 stream upload authority was revoked")
                part_number = len(parts) + 1
                response = await client.upload_part(
                    Bucket=s3_bucket, Key=s3_key, PartNumber=part_number, UploadId=upload_id, Body=bytes(buffer)
                )
                parts.append({"ETag": response["ETag"], "PartNumber": part_number})
                buffer.clear()

            async for chunk in chunks:
                buffer.extend(chunk)
                total_bytes += len(chunk)
                if len(buffer) >= _MULTIPART_PART_BYTES:
                    await _upload_part()

            if buffer or not parts:
                await _upload_part()

            if should_continue is not None and not should_continue():
                raise S3Error("S3 stream upload authority was revoked")

            await client.complete_multipart_upload(
                Bucket=s3_bucket, Key=s3_key, UploadId=upload_id, MultipartUpload={"Parts": parts}
            )
        except BaseException:
            with suppress(Exception):
                await client.abort_multipart_upload(Bucket=s3_bucket, Key=s3_key, UploadId=upload_id)
            raise
    return total_bytes


@handle_s3_error(message="Failed to download from S3")
async def download_from_s3(s3_key: str, aws: "AWSCredentials", s3_bucket: str) -> bytes:
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
    async with s3_client(aws) as client:
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
    s3_keys: AsyncIterable[str] | Iterable[str], aws: "AWSCredentials", s3_bucket: str
) -> AsyncIterator[tuple[str, bytes]]:
    """Download multiple objects over a single shared client (one connection pool).

    Accepts a sync or async iterable of keys, so it can stream lazily from
    list_s3_objects without materializing the full key list. Best-effort: keys
    that fail to download are logged and skipped. Yields ``(s3_key, content)``;
    each object is read fully into memory one at a time, so peak memory is
    bounded by the largest single object rather than the whole set.
    """
    async with s3_client(aws) as client:
        async for s3_key in _as_async_iter(s3_keys):
            try:
                response = await client.get_object(Bucket=s3_bucket, Key=s3_key)
                async with response["Body"] as stream:
                    yield s3_key, await stream.read()
            except (ClientError, BotoCoreError) as e:
                logger.warning(f"Failed to download {s3_key} from S3: {e}")


@handle_s3_error(message="Failed to delete from S3")
async def delete_from_s3(s3_key: str, aws: "AWSCredentials", s3_bucket: str) -> None:
    """
    Delete file from S3.

    Args:
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Raises:
        S3Error: If deletion fails due to AWS errors or network issues
    """
    async with s3_client(aws) as client:
        await client.delete_object(Bucket=s3_bucket, Key=s3_key)


async def copy_s3_object(source_key: str, dest_key: str, aws: "AWSCredentials", s3_bucket: str) -> None:
    """
    Copy an S3 object from source_key to dest_key within the same bucket.

    Raises:
        S3Error: If copy fails due to AWS errors or network issues
    """
    try:
        async with s3_client(aws) as client:
            await client.copy_object(
                Bucket=s3_bucket,
                CopySource={"Bucket": s3_bucket, "Key": source_key},
                Key=dest_key,
            )
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to copy S3 object from {source_key} to {dest_key}: {e}") from e


async def copy_agent_to_benchmark(benchmark_id: str, contract_name: str, aws: "AWSCredentials", s3_bucket: str) -> bool:
    """
    Freeze the agent for a benchmark run by copying
    agents/<name>.zip -> benchmarks/<benchmark_id>/<name>.zip.

    # NOTE: Skips if it already exists at that location

    Returns:
        True when this call created the benchmark copy, otherwise False.
    """
    source_key = get_contract_s3_key(contract_name)
    dest_key = get_benchmark_contract_s3_key(benchmark_id, contract_name)

    if await s3_object_exists(dest_key, aws, s3_bucket):
        return False

    await copy_s3_object(source_key, dest_key, aws, s3_bucket)
    return True


@handle_s3_error(message="Failed to check S3 object existence")
async def s3_object_exists(s3_key: str, aws: "AWSCredentials", s3_bucket: str) -> bool:
    """
    Check if an S3 object exists.

    Args:
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Returns:
        True if the object exists, False otherwise
    """
    async with s3_client(aws) as client:
        try:
            await client.head_object(Bucket=s3_bucket, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":  # pyright: ignore[reportTypedDictNotRequiredAccess]
                return False

            raise


async def list_s3_objects(prefix: str, aws: "AWSCredentials", s3_bucket: str) -> AsyncIterator[str]:
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
        async with s3_client(aws) as client:
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
                for s3_object in page.get("Contents", []):
                    if "Key" in s3_object:
                        yield s3_object["Key"]
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to list objects from S3: {e}") from e


@handle_s3_error(message="Failed to create presigned URL")
async def create_presigned_url(s3_key: str, aws: "AWSCredentials", s3_bucket: str, expiration: int = 86400) -> str:
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
    async with s3_client(aws) as client:
        presigned_url: str = await client.generate_presigned_url(
            "get_object",
            Params={"Bucket": s3_bucket, "Key": s3_key},
            ExpiresIn=expiration,
        )

    return presigned_url


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


def create_benchmark_url(benchmark_id: str, region: str, s3_bucket: str) -> str:
    """
    Create the AWS Console URL for a benchmark's S3 folder.

    Args:
        benchmark_id: Benchmark UUID as a string
        region: AWS region
        s3_bucket: S3 bucket name

    Returns:
        AWS Console URL pointing to the benchmark folder prefix
    """
    prefix = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/"
    return f"https://{region}.console.aws.amazon.com/s3/buckets/{s3_bucket}?region={region}&prefix={prefix}"


@handle_s3_error(message="Failed to list agents from S3")
async def list_agents(aws: "AWSCredentials", s3_bucket: str) -> list[tuple[str, datetime | None]]:
    """List zipped agent bundles under the `agents/` prefix.

    Returns (name, last_modified) pairs, one per `agents/<name>.zip`.

    Raises:
        S3Error: If listing fails due to AWS errors or network issues
    """
    agents: list[tuple[str, datetime | None]] = []
    async with s3_client(aws) as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=s3_bucket, Prefix="agents/"):
            for s3_object in page.get("Contents", []):
                tail = s3_object["Key"][len("agents/") :]
                if not tail.endswith(".zip"):
                    continue
                agents.append((tail[: -len(".zip")], s3_object.get("LastModified")))

    return agents
