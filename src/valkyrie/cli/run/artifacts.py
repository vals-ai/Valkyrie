import asyncio
from pathlib import Path
from typing import cast

from tracker import handle_s3_error
from tracker.exceptions import S3Error

from valkyrie.cli.s3_config import fetch_bucket_name, s3_client

_S3_DOWNLOAD_CONCURRENCY = 8


@handle_s3_error(message="Failed to download from S3")
async def download_s3_path(s3_path: str, output_dir: Path) -> int:
    """Download all objects under an S3 path prefix into output_dir. Returns count of files downloaded."""
    bucket_name = fetch_bucket_name()
    prefix = s3_path.rstrip("/") + "/" if not Path(s3_path).suffix else s3_path

    async with s3_client() as client:
        paginator = client.get_paginator("list_objects_v2")
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

            response = await client.get_object(Bucket=bucket_name, Key=key)
            dest.write_bytes(cast(bytes, await response["Body"].read()))

        for start in range(0, len(keys), _S3_DOWNLOAD_CONCURRENCY):
            batch = keys[start : start + _S3_DOWNLOAD_CONCURRENCY]
            await asyncio.gather(*(download_object(key) for key in batch))

        return len(keys)
