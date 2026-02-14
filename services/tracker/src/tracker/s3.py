"""S3 upload utilities for the tracker service."""

from functools import wraps

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from botocore.response import StreamingBody

from tracker.config import AWS_S3_BUCKET
from tracker.exceptions import S3Error

S3_CONTRACTS_PREFIX = "contracts"
S3_BENCHMARKS_PREFIX = "benchmarks"


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


@handle_s3_error(message=f"Failed to upload to S3 bucket '{AWS_S3_BUCKET}'")
def upload_to_s3(file_content: bytes, s3_key: str) -> None:
    """
    Upload file content to S3.

    Args:
        file_content: File content as bytes
        s3_key: S3 object key (path in bucket)

    Raises:
        S3Error: If upload fails due to AWS errors or network issues
    """

    s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
    s3_client.put_object(Bucket=AWS_S3_BUCKET, Key=s3_key, Body=file_content)


@handle_s3_error(message=f"Failed to download from S3 bucket '{AWS_S3_BUCKET}'")
def download_from_s3(s3_key: str) -> bytes:
    """
    Download file content from S3.

    Args:
        s3_key: S3 object key (path in bucket)

    Returns:
        File content as bytes

    Raises:
        S3Error: If download fails due to AWS errors, network issues, or file not found
    """

    s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=s3_key)

    return response["Body"].read()


@handle_s3_error(message=f"Failed to delete from S3 bucket '{AWS_S3_BUCKET}'")
def delete_from_s3(s3_key: str) -> None:
    """
    Delete file from S3.

    Args:
        s3_key: S3 object key (path in bucket)

    Raises:
        S3Error: If deletion fails due to AWS errors or network issues
    """

    s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
    s3_client.delete_object(Bucket=AWS_S3_BUCKET, Key=s3_key)


@handle_s3_error(message=f"Failed to stream download from S3 bucket '{AWS_S3_BUCKET}'")
def download_from_s3_stream(s3_key: str) -> tuple[StreamingBody, int]:
    """
    Download file content from S3 and return a streaming body and the content length.

    Args:
        s3_key: S3 object key (path in bucket)

    Returns:
        tuple[StreamingBody, int]: Streaming body and content length
    """

    s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
    response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=s3_key)

    body: StreamingBody = response["Body"]
    size = response["ContentLength"]

    return body, size


@handle_s3_error(message="Failed to check S3 object existence")
def s3_object_exists(s3_key: str) -> bool:
    """
    Check if an S3 object exists.

    Args:
        s3_key: S3 object key (path in bucket)

    Returns:
        True if the object exists, False otherwise
    """
    try:
        s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
        s3_client.head_object(Bucket=AWS_S3_BUCKET, Key=s3_key)
        return True
    except ClientError as e:
        # Acceptable error if object does not exist
        if e.response["Error"]["Code"] == "404":
            return False

        raise


@handle_s3_error(message=f"Failed to list objects from S3 bucket '{AWS_S3_BUCKET}'")
def list_s3_objects(prefix: str) -> list[str]:
    """
    List all S3 object keys with the given prefix.

    Args:
        prefix: S3 prefix to filter objects

    Returns:
        List of S3 object keys

    Raises:
        S3Error: If listing fails due to AWS errors or network issues
    """

    s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
    paginator = s3_client.get_paginator("list_objects_v2")

    object_keys: list[str] = []
    for page in paginator.paginate(Bucket=AWS_S3_BUCKET, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                if "Key" in obj:
                    object_keys.extend([obj["Key"]])

    return object_keys


@handle_s3_error(message=f"Failed to create presigned URL for S3 bucket '{AWS_S3_BUCKET}'")
def create_presigned_url(s3_key: str, expiration: int = 86400) -> str:
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

    s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
    presigned_url: str = s3_client.generate_presigned_url(
        "get_object", Params={"Bucket": AWS_S3_BUCKET, "Key": s3_key}, ExpiresIn=expiration
    )

    return presigned_url


@handle_s3_error(message=f"Failed to create console URL for S3 bucket '{AWS_S3_BUCKET}'")
def create_console_url(s3_key: str) -> str:
    """
    Create an AWS console URL for an S3 object.

    Args:
        s3_key: S3 object key (path in bucket)

    Returns:
        AWS console URL as a string
    """

    s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
    region = s3_client.meta.region_name  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]

    return f"https://{region}.console.aws.amazon.com/s3/object/{AWS_S3_BUCKET}?region={region}&prefix={s3_key}"
