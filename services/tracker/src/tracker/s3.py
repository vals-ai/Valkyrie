"""S3 upload utilities for the tracker service."""

from functools import wraps
from typing import TYPE_CHECKING, Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from botocore.response import StreamingBody

from tracker.exceptions import S3Error

if TYPE_CHECKING:
    from tracker.types import HarnessConfig

S3_CONTRACTS_PREFIX = "contracts"
S3_BENCHMARKS_PREFIX = "benchmarks"


def _s3_client(harness_config: "HarnessConfig") -> Any:
    """Create an S3 client from harness config credentials."""
    return boto3.client(  # pyright: ignore[reportUnknownMemberType]
        "s3",
        aws_access_key_id=harness_config.aws_access_key_id,
        aws_secret_access_key=harness_config.aws_secret_access_key,
        region_name=harness_config.aws_default_region,
    )


def get_contract_s3_key(contract_name: str) -> str:
    """Get the S3 key for a contract zip file."""
    return f"{S3_CONTRACTS_PREFIX}/{contract_name}.zip"


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


@handle_s3_error(message="Failed to upload to S3")
def upload_to_s3(file_content: bytes, s3_key: str, harness_config: "HarnessConfig") -> None:
    """
    Upload file content to S3.

    Args:
        file_content: File content as bytes
        s3_key: S3 object key (path in bucket)
        harness_config: Harness config providing credentials and bucket name

    Raises:
        S3Error: If upload fails due to AWS errors or network issues
    """
    client = _s3_client(harness_config)
    client.put_object(Bucket=harness_config.aws_s3_bucket, Key=s3_key, Body=file_content)


@handle_s3_error(message="Failed to download from S3")
def download_from_s3(s3_key: str, harness_config: "HarnessConfig") -> bytes:
    """
    Download file content from S3.

    Args:
        s3_key: S3 object key (path in bucket)
        harness_config: Harness config providing credentials and bucket name

    Returns:
        File content as bytes

    Raises:
        S3Error: If download fails due to AWS errors, network issues, or file not found
    """
    client = _s3_client(harness_config)
    response = client.get_object(Bucket=harness_config.aws_s3_bucket, Key=s3_key)

    return response["Body"].read()


@handle_s3_error(message="Failed to delete from S3")
def delete_from_s3(s3_key: str, harness_config: "HarnessConfig") -> None:
    """
    Delete file from S3.

    Args:
        s3_key: S3 object key (path in bucket)
        harness_config: Harness config providing credentials and bucket name

    Raises:
        S3Error: If deletion fails due to AWS errors or network issues
    """
    client = _s3_client(harness_config)
    client.delete_object(Bucket=harness_config.aws_s3_bucket, Key=s3_key)


@handle_s3_error(message="Failed to stream download from S3")
def download_from_s3_stream(s3_key: str, harness_config: "HarnessConfig") -> tuple[StreamingBody, int]:
    """
    Download file content from S3 and return a streaming body and the content length.

    Args:
        s3_key: S3 object key (path in bucket)
        harness_config: Harness config providing credentials and bucket name

    Returns:
        tuple[StreamingBody, int]: Streaming body and content length
    """
    client = _s3_client(harness_config)
    response = client.get_object(Bucket=harness_config.aws_s3_bucket, Key=s3_key)

    body: StreamingBody = response["Body"]
    size = response["ContentLength"]

    return body, size


@handle_s3_error(message="Failed to check S3 object existence")
def s3_object_exists(s3_key: str, harness_config: "HarnessConfig") -> bool:
    """
    Check if an S3 object exists.

    Args:
        s3_key: S3 object key (path in bucket)
        harness_config: Harness config providing credentials and bucket name

    Returns:
        True if the object exists, False otherwise
    """
    try:
        client = _s3_client(harness_config)
        client.head_object(Bucket=harness_config.aws_s3_bucket, Key=s3_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False

        raise


@handle_s3_error(message="Failed to list objects from S3")
def list_s3_objects(prefix: str, harness_config: "HarnessConfig") -> list[str]:
    """
    List all S3 object keys with the given prefix.

    Args:
        prefix: S3 prefix to filter objects
        harness_config: Harness config providing credentials and bucket name

    Returns:
        List of S3 object keys

    Raises:
        S3Error: If listing fails due to AWS errors or network issues
    """
    client = _s3_client(harness_config)
    paginator = client.get_paginator("list_objects_v2")

    object_keys: list[str] = []
    for page in paginator.paginate(Bucket=harness_config.aws_s3_bucket, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                if "Key" in obj:
                    object_keys.extend([obj["Key"]])

    return object_keys


@handle_s3_error(message="Failed to create presigned URL")
def create_presigned_url(s3_key: str, harness_config: "HarnessConfig", expiration: int = 86400) -> str:
    """
    Create a presigned URL for an S3 object.

    Args:
        s3_key: S3 object key (path in bucket)
        harness_config: Harness config providing credentials and bucket name
        expiration: URL expiration time in seconds (default: 1 day)

    Returns:
        Presigned URL as a string

    Raises:
        S3Error: If presigned URL creation fails
    """
    client = _s3_client(harness_config)
    presigned_url: str = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": harness_config.aws_s3_bucket, "Key": s3_key},
        ExpiresIn=expiration,
    )

    return presigned_url


@handle_s3_error(message="Failed to create console URL")
def create_console_url(s3_key: str, harness_config: "HarnessConfig") -> str:
    """
    Create an AWS console URL for an S3 object.

    Args:
        s3_key: S3 object key (path in bucket)
        harness_config: Harness config providing credentials, region, and bucket name

    Returns:
        AWS console URL as a string
    """
    region = harness_config.aws_default_region
    return f"https://{region}.console.aws.amazon.com/s3/object/{harness_config.aws_s3_bucket}?region={region}&prefix={s3_key}"


@handle_s3_error(message="Failed to create benchmark URL")
def create_benchmark_url(benchmark_id: str, harness_config: "HarnessConfig") -> str:
    """
    Create the AWS Console URL for a benchmark's S3 folder.

    Args:
        benchmark_id: Benchmark UUID as a string
        harness_config: Harness config providing credentials, region, and bucket name

    Returns:
        AWS Console URL pointing to the benchmark folder prefix
    """
    region = harness_config.aws_default_region
    prefix = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}/"
    return f"https://{region}.console.aws.amazon.com/s3/buckets/{harness_config.aws_s3_bucket}?region={region}&prefix={prefix}"
