"""S3 upload utilities for the tracker service."""

from collections.abc import AsyncGenerator, AsyncIterable, AsyncIterator, Awaitable, Callable, Coroutine, Iterable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from typing import Any, ParamSpec, TypeVar

import logfire
from botocore.exceptions import BotoCoreError, ClientError

from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.exceptions import S3Error
from tracker.logging import get_logger
from tracker.runtime.storage import ArtifactLocations, StoredObject, StoredObjectCopy

logger = get_logger(__name__)

_P = ParamSpec("_P")
_R = TypeVar("_R")

S3_AGENTS_PREFIX = "agents"
S3_BENCHMARKS_PREFIX = "benchmarks"

# S3 multipart uploads require every part except the last to be at least 5 MiB.
_MULTIPART_PART_BYTES = 8 * 1024 * 1024
_MAX_SINGLE_COPY_BYTES = 5 * 1024**3


@dataclass(frozen=True)
class S3ObjectCopy:
    """Identify an object created by a copy operation."""

    version_id: str | None


def s3_owner_arguments(runtime: AWSRuntime) -> dict[str, str]:
    """Add an account guard only for deployment-managed S3 calls."""
    if runtime.clients.credential_source != "managed":
        return {}

    if runtime.expected_bucket_owner is None:
        raise ValueError("Managed AWS runtime is missing the expected bucket owner")

    return {"ExpectedBucketOwner": runtime.expected_bucket_owner}


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


@logfire.instrument("upload_to_s3", extract_args=("s3_key",))
@handle_s3_error(message="Failed to upload to S3")
async def upload_to_s3(file_content: bytes, s3_key: str, runtime: AWSRuntime) -> None:
    """
    Upload file content to S3.

    Args:
        file_content: File content as bytes
        s3_key: S3 object key (path in bucket)
        runtime: AWS resources and clients for the operation

    Raises:
        S3Error: If upload fails due to AWS errors or network issues
    """
    async with runtime.clients.s3_client() as client:
        await client.put_object(
            Bucket=runtime.resources.s3_bucket,
            Key=s3_key,
            Body=file_content,
            **s3_owner_arguments(runtime),
        )


@logfire.instrument("upload_stream_to_s3", extract_args=("s3_key",))
@handle_s3_error(message="Failed to upload stream to S3")
async def upload_stream_to_s3(
    chunks: AsyncIterable[bytes],
    s3_key: str,
    runtime: AWSRuntime,
    should_continue: Callable[[], bool] | None = None,
    *,
    overwrite: bool = True,
) -> int:
    """
    Upload a byte stream to S3 via multipart upload, buffering at most one part in memory.

    Args:
        chunks: Async iterable of byte chunks to upload
        s3_key: S3 object key (path in bucket)
        runtime: AWS resources and clients for the operation
        should_continue: Optional authority check before each part and completion

    Returns:
        Total number of bytes uploaded

    Raises:
        S3Error: If upload fails due to AWS errors or network issues
    """
    total_bytes = 0
    s3_bucket = runtime.resources.s3_bucket
    owner_arguments = s3_owner_arguments(runtime)
    async with runtime.clients.s3_client() as client:
        multipart = await client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key, **owner_arguments)
        upload_id = multipart["UploadId"]
        try:
            parts: list[dict[str, Any]] = []
            buffer = bytearray()

            async def _upload_part() -> None:
                if should_continue is not None and not should_continue():
                    raise S3Error("S3 stream upload authority was revoked")
                part_number = len(parts) + 1
                response = await client.upload_part(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=bytes(buffer),
                    **owner_arguments,
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

            try:
                await client.complete_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                    **owner_arguments,
                    **({} if overwrite else {"IfNoneMatch": "*"}),
                )
            except ClientError as error:
                status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if not overwrite and status_code == 412:
                    raise FileExistsError(s3_key) from error
                # A concurrent conditional write returns 409; the key exists when that write won.
                if not overwrite and status_code == 409 and await s3_object_exists(s3_key, runtime):
                    raise FileExistsError(s3_key) from error
                raise
        except BaseException:
            with suppress(Exception):
                await client.abort_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=upload_id,
                    **owner_arguments,
                )
            raise
    return total_bytes


@handle_s3_error(message="Failed to download from S3")
async def download_from_s3(s3_key: str, runtime: AWSRuntime) -> bytes:
    """
    Download file content from S3.

    Args:
        s3_key: S3 object key (path in bucket)
        runtime: AWS resources and clients for the operation

    Returns:
        File content as bytes

    Raises:
        S3Error: If download fails due to AWS errors, network issues, or file not found
    """
    async with runtime.clients.s3_client() as client:
        response = await client.get_object(
            Bucket=runtime.resources.s3_bucket,
            Key=s3_key,
            **s3_owner_arguments(runtime),
        )
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
    s3_keys: AsyncIterable[str] | Iterable[str], runtime: AWSRuntime
) -> AsyncIterator[tuple[str, bytes]]:
    """Download multiple objects over a single shared client (one connection pool).

    Accepts a sync or async iterable of keys, so it can stream lazily from
    list_s3_objects without materializing the full key list. Best-effort: keys
    that fail to download are logged and skipped. Yields ``(s3_key, content)``;
    each object is read fully into memory one at a time, so peak memory is
    bounded by the largest single object rather than the whole set.
    """
    owner_arguments = s3_owner_arguments(runtime)
    async with runtime.clients.s3_client() as client:
        async for s3_key in _as_async_iter(s3_keys):
            try:
                response = await client.get_object(
                    Bucket=runtime.resources.s3_bucket,
                    Key=s3_key,
                    **owner_arguments,
                )
                async with response["Body"] as stream:
                    yield s3_key, await stream.read()
            except (ClientError, BotoCoreError) as e:
                logger.warning(f"Failed to download {s3_key} from S3: {e}")


@handle_s3_error(message="Failed to delete from S3")
async def delete_from_s3(s3_key: str, runtime: AWSRuntime, *, version_id: str | None = None) -> None:
    """
    Delete file from S3.

    Args:
        s3_key: S3 object key (path in bucket)
        runtime: AWS resources and clients for the operation

    Raises:
        S3Error: If deletion fails due to AWS errors or network issues
    """
    try:
        async with runtime.clients.s3_client() as client:
            if version_id is None:
                await client.delete_object(
                    Bucket=runtime.resources.s3_bucket,
                    Key=s3_key,
                    **s3_owner_arguments(runtime),
                )
            else:
                await client.delete_object(
                    Bucket=runtime.resources.s3_bucket,
                    Key=s3_key,
                    VersionId=version_id,
                    **s3_owner_arguments(runtime),
                )
    except (ClientError, BotoCoreError) as error:
        raise S3Error(f"Failed to delete object from S3: {error}") from error


async def copy_s3_object(source_key: str, dest_key: str, runtime: AWSRuntime) -> str | None:
    """
    Copy an S3 object from source_key to dest_key within the same bucket.

    Raises:
        S3Error: If copy fails due to AWS errors or network issues
    """
    try:
        async with runtime.clients.s3_client() as client:
            response = await client.copy_object(
                Bucket=runtime.resources.s3_bucket,
                CopySource={"Bucket": runtime.resources.s3_bucket, "Key": source_key},
                Key=dest_key,
                **s3_owner_arguments(runtime),
            )
            version_id = response.get("VersionId")
            return str(version_id) if version_id is not None else None
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to copy S3 object from {source_key} to {dest_key}: {e}") from e


async def copy_agent_to_benchmark(benchmark_id: str, contract_name: str, runtime: AWSRuntime) -> S3ObjectCopy | None:
    """
    Freeze the agent for a benchmark run by copying
    agents/<name>.zip -> benchmarks/<benchmark_id>/<name>.zip.

    # NOTE: Skips if it already exists at that location

    Returns:
        The created object identity, or None when the destination already exists.
    """
    source_key = get_contract_s3_key(contract_name)
    dest_key = get_benchmark_contract_s3_key(benchmark_id, contract_name)

    if await s3_object_exists(dest_key, runtime):
        return None

    version_id = await copy_s3_object(source_key, dest_key, runtime)
    return S3ObjectCopy(version_id=version_id)


@handle_s3_error(message="Failed to check S3 object existence")
async def s3_object_exists(s3_key: str, runtime: AWSRuntime) -> bool:
    """
    Check if an S3 object exists.

    Args:
        s3_key: S3 object key (path in bucket)
        runtime: AWS resources and clients for the operation

    Returns:
        True if the object exists, False otherwise
    """
    async with runtime.clients.s3_client() as client:
        try:
            await client.head_object(
                Bucket=runtime.resources.s3_bucket,
                Key=s3_key,
                **s3_owner_arguments(runtime),
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":  # pyright: ignore[reportTypedDictNotRequiredAccess]
                return False

            raise


async def list_s3_objects(prefix: str, runtime: AWSRuntime) -> AsyncIterator[str]:
    """
    Yield S3 object keys with the given prefix, a page at a time (no full list held in memory).

    Args:
        prefix: S3 prefix to filter objects
        runtime: AWS resources and clients for the operation

    Yields:
        S3 object keys

    Raises:
        S3Error: If listing fails due to AWS errors or network issues
    """
    try:
        async with runtime.clients.s3_client() as client:
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(
                Bucket=runtime.resources.s3_bucket,
                Prefix=prefix,
                **s3_owner_arguments(runtime),
            ):
                for s3_object in page.get("Contents", []):
                    if "Key" in s3_object:
                        yield s3_object["Key"]
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to list objects from S3: {e}") from e


@handle_s3_error(message="Failed to create presigned URL")
async def create_presigned_url(s3_key: str, runtime: AWSRuntime, expiration: int = 86400) -> str:
    """
    Create a presigned URL for an S3 object.

    Args:
        s3_key: S3 object key (path in bucket)
        runtime: AWS resources and clients for the operation
        expiration: URL expiration time in seconds (default: 1 day)

    Returns:
        Presigned URL as a string

    Raises:
        S3Error: If presigned URL creation fails
    """
    actual_expiration = runtime.clients.maximum_presign_ttl(expiration)
    async with runtime.clients.s3_client() as client:
        presigned_url: str = await client.generate_presigned_url(
            "get_object",
            Params={"Bucket": runtime.resources.s3_bucket, "Key": s3_key},
            ExpiresIn=actual_expiration,
        )

    return presigned_url


def create_console_url(s3_key: str, resources: AWSResources) -> str:
    """
    Create an AWS console URL for an S3 object.

    Args:
        s3_key: S3 object key (path in bucket)
        resources: AWS resource locations

    Returns:
        AWS console URL as a string
    """
    return (
        f"https://{resources.region}.console.aws.amazon.com/s3/object/{resources.s3_bucket}"
        f"?region={resources.region}&prefix={s3_key}"
    )


def create_benchmark_url(benchmark_id: str, resources: AWSResources) -> str:
    """
    Create the AWS Console URL for a benchmark's S3 folder.

    Args:
        benchmark_id: Benchmark UUID as a string
        resources: AWS resource locations

    Returns:
        AWS Console URL pointing to the benchmark folder prefix
    """
    prefix = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/"
    return (
        f"https://{resources.region}.console.aws.amazon.com/s3/buckets/{resources.s3_bucket}"
        f"?region={resources.region}&prefix={prefix}"
    )


@handle_s3_error(message="Failed to list agents from S3")
async def list_agents(runtime: AWSRuntime) -> list[tuple[str, datetime | None]]:
    """List zipped agent bundles under the `agents/` prefix.

    Returns (name, last_modified) pairs, one per `agents/<name>.zip`.

    Raises:
        S3Error: If listing fails due to AWS errors or network issues
    """
    agents: list[tuple[str, datetime | None]] = []
    async with runtime.clients.s3_client() as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(
            Bucket=runtime.resources.s3_bucket,
            Prefix="agents/",
            **s3_owner_arguments(runtime),
        ):
            for s3_object in page.get("Contents", []):
                tail = s3_object["Key"][len("agents/") :]
                if not tail.endswith(".zip"):
                    continue
                agents.append((tail[: -len(".zip")], s3_object.get("LastModified")))

    return agents


class S3ObjectCopier:
    """Copy one object between two S3 runtimes with separate authority."""

    def __init__(self, source: AWSRuntime, destination: AWSRuntime) -> None:
        self._source = source
        self._destination = destination

    @handle_s3_error(message="Failed to copy S3 object")
    async def copy(self, source_key: str, destination_key: str) -> StoredObjectCopy:
        source_owner_arguments = s3_owner_arguments(self._source)
        async with self._source.clients.s3_client() as source_client:
            source_head = await source_client.head_object(
                Bucket=self._source.resources.s3_bucket,
                Key=source_key,
                **source_owner_arguments,
            )

        if source_head["ContentLength"] > _MAX_SINGLE_COPY_BYTES:
            raise S3Error(f"Agent bundle exceeds the 5 GiB single-copy limit: {source_key}")

        copy_owner_arguments = s3_owner_arguments(self._destination)
        if expected_source_owner := source_owner_arguments.get("ExpectedBucketOwner"):
            copy_owner_arguments["ExpectedSourceBucketOwner"] = expected_source_owner

        async with self._destination.clients.s3_client() as destination_client:
            response = await destination_client.copy_object(
                Bucket=self._destination.resources.s3_bucket,
                Key=destination_key,
                CopySource={"Bucket": self._source.resources.s3_bucket, "Key": source_key},
                CopySourceIfMatch=source_head["ETag"],
                **copy_owner_arguments,
            )

        version_id = response.get("VersionId")
        if self._destination.clients.credential_source == "managed" and (
            not isinstance(version_id, str) or not version_id.strip() or version_id.strip().lower() == "null"
        ):
            raise S3Error("Managed S3 copy did not return a destination version")

        return StoredObjectCopy(deletion_token=str(version_id) if version_id is not None else None)


class _S3ObjectReadSession:
    """S3 reads scoped to an already-open client."""

    def __init__(self, client: Any, bucket: str, owner_arguments: dict[str, str]) -> None:
        self._client = client
        self._bucket = bucket
        self._owner_arguments = owner_arguments

    @handle_s3_error(message="Failed to download from S3")
    async def get_bytes(self, key: str) -> bytes:
        response = await self._client.get_object(Bucket=self._bucket, Key=key, **self._owner_arguments)
        async with response["Body"] as stream:
            return await stream.read()

    async def list_objects(self, prefix: str) -> AsyncIterator[StoredObject]:
        try:
            paginator = self._client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix, **self._owner_arguments):
                for stored_object in page.get("Contents", []):
                    key = stored_object.get("Key")
                    if key is not None:
                        yield StoredObject(
                            key=key, last_modified=stored_object.get("LastModified"), size=stored_object["Size"]
                        )
        except (ClientError, BotoCoreError) as error:
            raise S3Error(f"Failed to list objects from S3: {error}") from error


class S3ObjectStore:
    """Object-store adapter using already-resolved AWS authority."""

    def __init__(self, runtime: AWSRuntime) -> None:
        self._runtime = runtime

    @asynccontextmanager
    async def read_session(self) -> AsyncGenerator[_S3ObjectReadSession, None]:
        async with self._runtime.clients.s3_client() as client:
            yield _S3ObjectReadSession(
                client,
                self._runtime.resources.s3_bucket,
                s3_owner_arguments(self._runtime),
            )

    async def put_bytes(self, key: str, content: bytes) -> None:
        await upload_to_s3(content, key, self._runtime)

    async def put_stream(
        self,
        key: str,
        chunks: AsyncIterable[bytes],
        *,
        should_continue: Callable[[], bool] | None = None,
        overwrite: bool = True,
    ) -> int:
        return await upload_stream_to_s3(
            chunks, key, self._runtime, should_continue=should_continue, overwrite=overwrite
        )

    async def get_bytes(self, key: str) -> bytes:
        async with self.read_session() as reader:
            return await reader.get_bytes(key)

    async def get_many(self, keys: AsyncIterable[str]) -> AsyncIterator[tuple[str, bytes]]:
        owner_arguments = s3_owner_arguments(self._runtime)
        async with self._runtime.clients.s3_client() as client:
            async for key in keys:
                try:
                    response = await client.get_object(
                        Bucket=self._runtime.resources.s3_bucket,
                        Key=key,
                        **owner_arguments,
                    )
                    async with response["Body"] as stream:
                        yield key, await stream.read()
                except (ClientError, BotoCoreError) as error:
                    logger.warning(f"Failed to download {key} from S3: {error}")

    async def delete(self, key: str, *, deletion_token: str | None = None) -> None:
        await delete_from_s3(key, self._runtime, version_id=deletion_token)

    async def copy(self, source_key: str, destination_key: str) -> StoredObjectCopy:
        return StoredObjectCopy(deletion_token=await copy_s3_object(source_key, destination_key, self._runtime))

    async def exists(self, key: str) -> bool:
        return await s3_object_exists(key, self._runtime)

    async def list_objects(self, prefix: str) -> AsyncIterator[StoredObject]:
        async with self.read_session() as reader:
            async for stored_object in reader.list_objects(prefix):
                yield stored_object

    async def stat(self, key: str) -> StoredObject:
        async with self._runtime.clients.s3_client() as client:
            response = await client.head_object(
                Bucket=self._runtime.resources.s3_bucket,
                Key=key,
                **s3_owner_arguments(self._runtime),
            )
        return StoredObject(key, response.get("LastModified"), size=response["ContentLength"])

    async def list_objects_page(
        self, prefix: str, *, cursor: str | None, limit: int
    ) -> tuple[list[StoredObject], str | None]:
        arguments: dict[str, Any] = {"Bucket": self._runtime.resources.s3_bucket, "Prefix": prefix, "MaxKeys": limit}
        arguments.update(s3_owner_arguments(self._runtime))
        if cursor is not None:
            arguments["ContinuationToken"] = cursor
        async with self._runtime.clients.s3_client() as client:
            response = await client.list_objects_v2(**arguments)
        return [
            StoredObject(item["Key"], item.get("LastModified"), size=item["Size"])
            for item in response.get("Contents", [])
        ], response.get("NextContinuationToken")

    async def temporary_download_url(self, key: str, *, expires_in: int) -> str:
        return await create_presigned_url(key, self._runtime, expiration=expires_in)


class S3ArtifactLocations(ArtifactLocations):
    """AWS-console artifact locations for resolved S3 resources."""

    def __init__(self, resources: AWSResources) -> None:
        self._resources = resources

    def object_location(self, key: str) -> str:
        return create_console_url(key, self._resources)

    def prefix_location(self, prefix: str) -> str:
        return (
            f"https://{self._resources.region}.console.aws.amazon.com/s3/buckets/{self._resources.s3_bucket}"
            f"?region={self._resources.region}&prefix={prefix}"
        )
