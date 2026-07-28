import os
import zipfile
from io import BytesIO

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from mypy_boto3_s3 import S3Client

from tracker.aws.s3 import get_contract_s3_key
from tracker.types import AWSCredentials


def integration_test_agent_name(worker_id: str) -> str:
    run_id = os.getenv("GITHUB_RUN_ID") or "local"
    attempt = os.getenv("GITHUB_RUN_ATTEMPT") or "0"
    return f"dummy-{run_id}-{attempt}-{worker_id}"


def build_test_agent_zip(contract_name: str) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"{contract_name}/README.txt", "integration test agent\n")
    return buffer.getvalue()


def create_s3_client(aws: AWSCredentials) -> S3Client:
    return boto3.client(  # pyright: ignore[reportUnknownMemberType]
        "s3",
        aws_access_key_id=aws.aws_access_key_id,
        aws_secret_access_key=aws.aws_secret_access_key,
        aws_session_token=aws.aws_session_token,
        region_name=aws.aws_default_region,
        config=Config(max_pool_connections=20),
    )


def seed_test_agent_artifact(s3_client: S3Client, s3_bucket: str, contract_name: str) -> str:
    key = get_contract_s3_key(contract_name)

    try:
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=build_test_agent_zip(contract_name),
            ContentType="application/zip",
        )
        s3_client.head_object(Bucket=s3_bucket, Key=key)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(
            "Integration tests need TEST_AWS_S3_BUCKET to be writable and readable by the configured AWS "
            f"credentials. Failed to seed s3://{s3_bucket}/{key}."
        ) from exc

    return key


def delete_test_agent_artifact(s3_client: S3Client, s3_bucket: str, key: str) -> None:
    try:
        s3_client.delete_object(Bucket=s3_bucket, Key=key)
    except (BotoCoreError, ClientError) as exc:
        raise RuntimeError(f"Failed to delete seeded integration test artifact s3://{s3_bucket}/{key}.") from exc
