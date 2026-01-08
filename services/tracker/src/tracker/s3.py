"""S3 upload utilities for the tracker service."""

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from tracker.config import S3_BUCKET_NAME
from tracker.exceptions import S3Error

S3_CONTRACTS_PREFIX = "contracts"


def get_contract_s3_key(contract_name: str) -> str:
    """Get the S3 key for a contract zip file."""
    return f"{S3_CONTRACTS_PREFIX}/{contract_name}.zip"


def upload_to_s3(file_content: bytes, s3_key: str) -> None:
    """
    Upload file content to S3.

    Args:
        file_content: File content as bytes
        s3_key: S3 object key (path in bucket)

    Raises:
        S3Error: If upload fails due to AWS errors or network issues
    """
    bucket_name = S3_BUCKET_NAME

    try:
        s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=file_content)
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to upload to S3 bucket '{bucket_name}' with key '{s3_key}': {str(e)}") from e


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
    bucket_name = S3_BUCKET_NAME

    try:
        s3_client = boto3.client("s3")  # pyright: ignore[reportUnknownMemberType]
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        return response["Body"].read()
    except (ClientError, BotoCoreError) as e:
        raise S3Error(f"Failed to download from S3 bucket '{bucket_name}' with key '{s3_key}': {str(e)}") from e
