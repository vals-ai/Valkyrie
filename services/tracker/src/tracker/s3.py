"""S3 upload utilities for the tracker service."""

import asyncio
from functools import lru_cache, wraps
from typing import TYPE_CHECKING, Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from botocore.response import StreamingBody

from tracker.exceptions import S3Error

if TYPE_CHECKING:
    from tracker.types import AWSCredentials

S3_AGENTS_PREFIX = "agents"
S3_BENCHMARKS_PREFIX = "benchmarks"


@lru_cache(maxsize=32)
def _s3_client(aws: "AWSCredentials") -> Any:
    """S3 client cached to share instances."""
    return boto3.client(  # pyright: ignore[reportUnknownMemberType]
        "s3",
        aws_access_key_id=aws.aws_access_key_id,
        aws_secret_access_key=aws.aws_secret_access_key,
        region_name=aws.aws_default_region,
        config=Config(max_pool_connections=200),
    )


def get_contract_s3_key(contract_name: str) -> str:
    """Get the S3 key for an agent zip file."""
    return f"{S3_AGENTS_PREFIX}/{contract_name}.zip"


def get_agent_result_s3_key(benchmark_id: str, task_id: str, output_name: str) -> str:
    """Get the S3 key for an agent output archive."""
    return f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/{task_id}/{output_name}"


def handle_s3_error(message: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (ClientError, BotoCoreError) as e:
                raise S3Error(f"{message}: {e}") from e

        return wrapper

    return decorator


async def upload_to_s3(file_content: bytes, s3_key: str, aws: "AWSCredentials", s3_bucket: str) -> None:
    """
    Upload file content to S3 without blocking the event loop.

    Args:
        file_content: File content as bytes
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Raises:
        S3Error: If upload fails due to AWS errors or network issues
    """

    def _upload() -> None:
        client = _s3_client(aws)
        client.put_object(Bucket=s3_bucket, Key=s3_key, Body=file_content)

    try:
        await asyncio.to_thread(_upload)
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to upload to S3: {e}") from e


@handle_s3_error(message="Failed to download from S3")
def download_from_s3(s3_key: str, aws: "AWSCredentials", s3_bucket: str) -> bytes:
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
    client = _s3_client(aws)
    response = client.get_object(Bucket=s3_bucket, Key=s3_key)

    return response["Body"].read()


@handle_s3_error(message="Failed to delete from S3")
def delete_from_s3(s3_key: str, aws: "AWSCredentials", s3_bucket: str) -> None:
    """
    Delete file from S3.

    Args:
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Raises:
        S3Error: If deletion fails due to AWS errors or network issues
    """
    client = _s3_client(aws)
    client.delete_object(Bucket=s3_bucket, Key=s3_key)


@handle_s3_error(message="Failed to stream download from S3")
def download_from_s3_stream(s3_key: str, aws: "AWSCredentials", s3_bucket: str) -> tuple[StreamingBody, int]:
    """
    Download file content from S3 and return a streaming body and the content length.

    Args:
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Returns:
        tuple[StreamingBody, int]: Streaming body and content length
    """
    client = _s3_client(aws)
    response = client.get_object(Bucket=s3_bucket, Key=s3_key)

    body: StreamingBody = response["Body"]
    size = response["ContentLength"]

    return body, size


@handle_s3_error(message="Failed to check S3 object existence")
def s3_object_exists(s3_key: str, aws: "AWSCredentials", s3_bucket: str) -> bool:
    """
    Check if an S3 object exists.

    Args:
        s3_key: S3 object key (path in bucket)
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Returns:
        True if the object exists, False otherwise
    """
    try:
        client = _s3_client(aws)
        client.head_object(Bucket=s3_bucket, Key=s3_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False

        raise


@handle_s3_error(message="Failed to list objects from S3")
def list_s3_objects(prefix: str, aws: "AWSCredentials", s3_bucket: str) -> list[str]:
    """
    List all S3 object keys with the given prefix.

    Args:
        prefix: S3 prefix to filter objects
        aws: AWS credentials for authentication
        s3_bucket: S3 bucket name

    Returns:
        List of S3 object keys

    Raises:
        S3Error: If listing fails due to AWS errors or network issues
    """
    client = _s3_client(aws)
    paginator = client.get_paginator("list_objects_v2")

    object_keys: list[str] = []
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                if "Key" in obj:
                    object_keys.extend([obj["Key"]])

    return object_keys


@handle_s3_error(message="Failed to create presigned URL")
def create_presigned_url(s3_key: str, aws: "AWSCredentials", s3_bucket: str, expiration: int = 86400) -> str:
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
    client = _s3_client(aws)
    presigned_url: str = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": s3_bucket, "Key": s3_key},
        ExpiresIn=expiration,
    )

    return presigned_url


@handle_s3_error(message="Failed to create console URL")
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


@handle_s3_error(message="Failed to create run URL")
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
