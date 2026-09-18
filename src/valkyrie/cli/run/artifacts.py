import asyncio
from pathlib import Path
from typing import cast

from tracker import handle_s3_error
from tracker.exceptions import S3Error

from valkyrie.cli import s3_config as cli_s3

_S3_DOWNLOAD_CONCURRENCY = 8


@handle_s3_error(message="Failed to download from S3")
async def download_s3_path(s3_path: str, output_dir: Path) -> int:
    """Download all objects under an S3 path prefix into output_dir. Returns count of files downloaded."""
    bucket_name = cli_s3.fetch_bucket_name()
    prefix = s3_path.rstrip("/") + "/" if not Path(s3_path).suffix else s3_path

    async with cli_s3.s3_client() as client:
        paginator = client.get_paginator("list_objects_v2")
        keys: list[str] = []
        async for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(cast(str, obj["Key"]))

        if not keys:
            raise S3Error(f"No files found at '{s3_path}' in bucket '{bucket_name}'")

        output_dir = output_dir.resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        async def download_object(key: str) -> None:
            relative = key.removeprefix(prefix).lstrip("/")
            destination = (output_dir / relative if relative else output_dir / Path(key).name).resolve()
            if not destination.is_relative_to(output_dir):
                raise S3Error(f"Requested path is not relative the output directory '{key}'")
            destination.parent.mkdir(parents=True, exist_ok=True)

            response = await client.get_object(Bucket=bucket_name, Key=key)
            destination.write_bytes(cast(bytes, await response["Body"].read()))

        for start in range(0, len(keys), _S3_DOWNLOAD_CONCURRENCY):
            batch = keys[start : start + _S3_DOWNLOAD_CONCURRENCY]
            await asyncio.gather(*(download_object(key) for key in batch))

        return len(keys)
