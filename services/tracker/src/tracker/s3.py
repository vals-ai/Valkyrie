"""S3 upload utilities for the tracker service."""

from functools import wraps
from typing import Any, Callable, Coroutine

import aiobotocore.session
from botocore.exceptions import BotoCoreError, ClientError

from tracker.config import AWS_S3_BUCKET
from tracker.exceptions import S3Error

_session = aiobotocore.session.get_session()  # pyright: ignore[reportUnknownMemberType]

S3_CONTRACTS_PREFIX = "contracts"
S3_BENCHMARKS_PREFIX = "benchmarks"


def get_contract_s3_key(contract_name: str) -> str:
    """Get the S3 key for a contract zip file."""
    return f"{S3_CONTRACTS_PREFIX}/{contract_name}.zip"


def get_agent_result_s3_key(benchmark_id: str, task_id: str, output_name: str) -> str:
    """Get the S3 key for an agent output archive."""
    return f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{task_id}/{output_name}"


def handle_s3_error(message: str) -> Callable[..., Callable[..., Coroutine[Any, Any, Any]]]:
    def decorator(func: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., Coroutine[Any, Any, Any]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await func(*args, **kwargs)
            except (ClientError, BotoCoreError) as e:
                raise S3Error(f"{message}: {e}") from e

        return wrapper

    return decorator


@handle_s3_error(message=f"Failed to upload to S3 bucket '{AWS_S3_BUCKET}'")
async def upload_to_s3(file_content: bytes, s3_key: str) -> None:
    """
    Upload file content to S3.

    Args:
        file_content: File content as bytes
        s3_key: S3 object key (path in bucket)

    Raises:
        S3Error: If upload fails due to AWS errors or network issues
    """
    async with _session.create_client("s3") as client:  # pyright: ignore[reportUnknownMemberType]
        await client.put_object(Bucket=AWS_S3_BUCKET, Key=s3_key, Body=file_content)  # pyright: ignore


@handle_s3_error(message=f"Failed to download from S3 bucket '{AWS_S3_BUCKET}'")
async def download_from_s3(s3_key: str) -> bytes:
    """
    Download file content from S3.

    Args:
        s3_key: S3 object key (path in bucket)

    Returns:
        File content as bytes

    Raises:
        S3Error: If download fails due to AWS errors, network issues, or file not found
    """
    async with _session.create_client("s3") as client:  # pyright: ignore[reportUnknownMemberType]
        response = await client.get_object(Bucket=AWS_S3_BUCKET, Key=s3_key)  # pyright: ignore
        return await response["Body"].read()  # pyright: ignore


@handle_s3_error(message=f"Failed to delete from S3 bucket '{AWS_S3_BUCKET}'")
async def delete_from_s3(s3_key: str) -> None:
    """
    Delete file from S3.

    Args:
        s3_key: S3 object key (path in bucket)

    Raises:
        S3Error: If deletion fails due to AWS errors or network issues
    """
    async with _session.create_client("s3") as client:  # pyright: ignore[reportUnknownMemberType]
        await client.delete_object(Bucket=AWS_S3_BUCKET, Key=s3_key)  # pyright: ignore


@handle_s3_error(message=f"Failed to stream download from S3 bucket '{AWS_S3_BUCKET}'")
async def download_from_s3_stream(s3_key: str) -> tuple[bytes, int]:
    """
    Download file content from S3 and return the content and the content length.

    Args:
        s3_key: S3 object key (path in bucket)

    Returns:
        tuple[bytes, int]: File content and content length
    """
    async with _session.create_client("s3") as client:  # pyright: ignore[reportUnknownMemberType]
        response = await client.get_object(Bucket=AWS_S3_BUCKET, Key=s3_key)  # pyright: ignore
        body = bytes(await response["Body"].read())  # pyright: ignore
        size = int(response["ContentLength"])  # pyright: ignore
        return body, size


@handle_s3_error(message="Failed to check S3 object existence")
async def s3_object_exists(s3_key: str) -> bool:
    """
    Check if an S3 object exists.

    Args:
        s3_key: S3 object key (path in bucket)

    Returns:
        True if the object exists, False otherwise
    """
    try:
        async with _session.create_client("s3") as client:  # pyright: ignore[reportUnknownMemberType]
            await client.head_object(Bucket=AWS_S3_BUCKET, Key=s3_key)  # pyright: ignore
            return True
    except ClientError as e:
        # Acceptable error if object does not exist
        if e.response.get("Error", {}).get("Code") == "404":
            return False

        raise


@handle_s3_error(message=f"Failed to list objects from S3 bucket '{AWS_S3_BUCKET}'")
async def list_s3_objects(prefix: str) -> list[str]:
    """
    List all S3 object keys with the given prefix.

    Args:
        prefix: S3 prefix to filter objects

    Returns:
        List of S3 object keys

    Raises:
        S3Error: If listing fails due to AWS errors or network issues
    """
    async with _session.create_client("s3") as client:  # pyright: ignore[reportUnknownMemberType]
        paginator = client.get_paginator("list_objects_v2")  # pyright: ignore

        object_keys: list[str] = []
        async for page in paginator.paginate(Bucket=AWS_S3_BUCKET, Prefix=prefix):  # pyright: ignore
            if "Contents" in page:
                for obj in page["Contents"]:  # pyright: ignore
                    if "Key" in obj:
                        object_keys.append(obj["Key"])  # pyright: ignore

        return object_keys


@handle_s3_error(message=f"Failed to create presigned URL for S3 bucket '{AWS_S3_BUCKET}'")
async def create_presigned_url(s3_key: str, expiration: int = 86400) -> str:
    """
    Create a presigned URL for an S3 object.

    Args:
        s3_key: S3 object key (path in bucket)
        expiration: URL expiration time in seconds (default: 1 day)

    Returns:
        Presigned URL as a string

    Raises:
        S3Error: If presigned URL creation fails
    """
    async with _session.create_client("s3") as client:  # pyright: ignore[reportUnknownMemberType]
        presigned_url = str(
            await client.generate_presigned_url(  # pyright: ignore
                "get_object", Params={"Bucket": AWS_S3_BUCKET, "Key": s3_key}, ExpiresIn=expiration
            )
        )
        return presigned_url


def create_console_url(s3_key: str) -> str:
    """
    Create an AWS console URL for an S3 object.

    Args:
        s3_key: S3 object key (path in bucket)

    Returns:
        AWS console URL as a string
    """
    region = _session.get_config_variable("region")  # pyright: ignore[reportUnknownMemberType]

    return f"https://{region}.console.aws.amazon.com/s3/object/{AWS_S3_BUCKET}?region={region}&prefix={s3_key}"
