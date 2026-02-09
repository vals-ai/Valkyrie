"""S3 upload utilities for the tracker service."""

import boto3
from botocore.exceptions import BotoCoreError, ClientError

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
    try:
        s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
        s3_client.delete_object(Bucket=AWS_S3_BUCKET, Key=s3_key)
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to delete from S3 bucket '{AWS_S3_BUCKET}' with key '{s3_key}': {str(e)}") from e
