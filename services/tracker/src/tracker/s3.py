"""S3 upload utilities for the tracker service."""

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


def upload_to_s3(file_content: bytes, s3_key: str) -> None:
    """
    Upload file content to S3.

    Args:
        file_content: File content as bytes
        s3_key: S3 object key (path in bucket)

    Raises:
        S3Error: If upload fails due to AWS errors or network issues
    """

    try:
        s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
        s3_client.put_object(Bucket=AWS_S3_BUCKET, Key=s3_key, Body=file_content)
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to upload to S3 bucket '{AWS_S3_BUCKET}' with key '{s3_key}': {str(e)}") from e


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

    try:
        s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
        response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=s3_key)
        return response["Body"].read()
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to download from S3 bucket '{AWS_S3_BUCKET}' with key '{s3_key}': {str(e)}") from e


def delete_from_s3(s3_key: str) -> None:
    """
    Delete file from S3.

    Args:
        s3_key: S3 object key (path in bucket)

    Raises:
        S3Error: If deletion fails due to AWS errors or network issues
    """
    try:
        s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
        s3_client.delete_object(Bucket=AWS_S3_BUCKET, Key=s3_key)
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to delete from S3 bucket '{AWS_S3_BUCKET}' with key '{s3_key}': {str(e)}") from e


def download_from_s3_stream(s3_key: str) -> tuple[StreamingBody, int]:
    """
    Download file content from S3 and return a streaming body and the content length.

    Args:
        s3_key: S3 object key (path in bucket)

    Returns:
        tuple[StreamingBody, int]: Streaming body and content length
    """
    try:
        s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
        response = s3_client.get_object(Bucket=AWS_S3_BUCKET, Key=s3_key)

        body: StreamingBody = response["Body"]
        size = response["ContentLength"]

        return body, size

    except (ClientError, BotoCoreError) as e:
        raise S3Error(
            f"Failed to stream download from S3 bucket '{AWS_S3_BUCKET}' with key '{s3_key}': {str(e)}"
        ) from e


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
    try:
        s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
        paginator = s3_client.get_paginator("list_objects_v2")

        object_keys: list[str] = []
        for page in paginator.paginate(Bucket=AWS_S3_BUCKET, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    if "Key" in obj:
                        object_keys.extend([obj["Key"]])

        return object_keys
    except (ClientError, BotoCoreError) as e:
        raise S3Error(
            f"Failed to list objects from S3 bucket '{AWS_S3_BUCKET}' with prefix '{prefix}': {str(e)}"
        ) from e
