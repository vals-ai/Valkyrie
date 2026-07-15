from datetime import datetime
from functools import lru_cache
from typing import Any

import aioboto3
import click
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from tracker import handle_s3_error
from tracker.exceptions import S3Error

from valkyrie.cli.config.state import load_config

_CLIENT_CONFIG = Config(max_pool_connections=200, retries={"mode": "standard"})


def fetch_bucket_name() -> str:
    config = load_config()
    bucket_name = config.get("S3_BUCKET")
    if not bucket_name:
        raise click.ClickException("S3_BUCKET key not found. Add it using 'valkyrie config set' first.")

    return bucket_name


@lru_cache(maxsize=4)
def _s3_session(access_key_id: str, secret_access_key: str, region_name: str) -> aioboto3.Session:
    return aioboto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region_name,
    )


def s3_client() -> Any:
    """Create an async S3 client using credentials from the valkyrie config."""
    config = load_config()
    session = _s3_session(
        config["AWS_ACCESS_KEY_ID"],
        config["AWS_SECRET_ACCESS_KEY"],
        config["AWS_DEFAULT_REGION"],
    )
    return session.client("s3", config=_CLIENT_CONFIG)


async def copy_s3_object(source_key: str, dest_key: str, bucket_name: str) -> None:
    """Copy an S3 object within one bucket."""
    try:
        async with s3_client() as client:
            await client.copy_object(
                Bucket=bucket_name,
                CopySource={"Bucket": bucket_name, "Key": source_key},
                Key=dest_key,
            )
    except (ClientError, BotoCoreError) as exc:
        raise S3Error(f"Failed to copy S3 object from {source_key} to {dest_key}: {exc}") from exc


@handle_s3_error(message="Failed to download from S3")
async def download_from_s3(s3_key: str, bucket_name: str) -> bytes:
    """Download one S3 object."""
    async with s3_client() as client:
        response = await client.get_object(Bucket=bucket_name, Key=s3_key)
        async with response["Body"] as stream:
            return await stream.read()


@handle_s3_error(message="Failed to delete from S3")
async def delete_from_s3(s3_key: str, bucket_name: str) -> None:
    """Delete one S3 object."""
    async with s3_client() as client:
        await client.delete_object(Bucket=bucket_name, Key=s3_key)


@handle_s3_error(message="Failed to check S3 object existence")
async def s3_object_exists(s3_key: str, bucket_name: str) -> bool:
    """Return whether an S3 object exists in a bucket."""
    async with s3_client() as client:
        try:
            await client.head_object(Bucket=bucket_name, Key=s3_key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return False
            raise


@handle_s3_error(message="Failed to list agents from S3")
async def list_agents(bucket_name: str) -> list[tuple[str, datetime | None]]:
    """List zipped agent bundles under the agents prefix."""
    agents: list[tuple[str, datetime | None]] = []
    async with s3_client() as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=bucket_name, Prefix="agents/"):
            for s3_object in page.get("Contents", []):
                tail = s3_object["Key"][len("agents/") :]
                if not tail.endswith(".zip"):
                    continue
                agents.append((tail[: -len(".zip")], s3_object.get("LastModified")))

    return agents
