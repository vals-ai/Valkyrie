import asyncio
from pathlib import Path
from typing import cast

import aioboto3
import click
from tracker import handle_s3_error
from tracker.exceptions import S3Error

from valkyrie.cli.config.state import load_config

_S3_DOWNLOAD_CONCURRENCY = 8


def _fetch_bucket_name() -> str:
    config = load_config()
    bucket_name = config.get("S3_BUCKET")
    if not bucket_name:
        raise click.ClickException("S3_BUCKET key not found. Add it using 'valkyrie config set' first.")

    return bucket_name


def _s3_client():
    """Create an aioboto3 S3 client using credentials from the valkyrie config."""
    config = load_config()
    session = aioboto3.Session(
        aws_access_key_id=config.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config.get("AWS_SECRET_ACCESS_KEY"),
        region_name=config.get("AWS_DEFAULT_REGION"),
    )
    return session.client("s3")


@handle_s3_error(message="Failed to download from S3")
async def download_s3_path(s3_path: str, output_dir: Path) -> int:
    """Download all objects under an S3 path prefix into output_dir. Returns count of files downloaded."""
    bucket_name = _fetch_bucket_name()
    prefix = s3_path.rstrip("/") + "/" if not Path(s3_path).suffix else s3_path

    async with _s3_client() as s3_client:
        paginator = s3_client.get_paginator("list_objects_v2")
        keys: list[str] = []
        async for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(cast(str, obj["Key"]))

        if not keys:
            raise S3Error(f"No files found at '{s3_path}' in bucket '{bucket_name}'")

        output_dir.mkdir(parents=True, exist_ok=True)

        async def download_object(key: str) -> None:
            relative = key.removeprefix(prefix).lstrip("/")
            dest = output_dir / relative if relative else output_dir / Path(key).name
            dest.parent.mkdir(parents=True, exist_ok=True)

            response = await s3_client.get_object(Bucket=bucket_name, Key=key)
            dest.write_bytes(cast(bytes, await response["Body"].read()))

        for start in range(0, len(keys), _S3_DOWNLOAD_CONCURRENCY):
            batch = keys[start : start + _S3_DOWNLOAD_CONCURRENCY]
            await asyncio.gather(*(download_object(key) for key in batch))

        return len(keys)
