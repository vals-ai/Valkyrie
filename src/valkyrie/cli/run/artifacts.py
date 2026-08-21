from pathlib import Path
from typing import cast

from tracker import handle_s3_error
from tracker.aws.s3 import normalize_s3_download_prefix
from tracker.exceptions import S3Error

from valkyrie.cli import s3_config as cli_s3
from valkyrie.cli.remote_storage import gather_in_batches, resolve_download_destination


@handle_s3_error(message="Failed to download from S3")
async def download_s3_path(s3_path: str, output_dir: Path) -> int:
    """Download all objects under an S3 path prefix into output_dir. Returns count of files downloaded."""
    bucket_name = cli_s3.fetch_bucket_name()
    prefix = normalize_s3_download_prefix(s3_path)

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
            destination = resolve_download_destination(key, prefix, output_dir)

            response = await client.get_object(Bucket=bucket_name, Key=key)
            destination.write_bytes(cast(bytes, await response["Body"].read()))

        await gather_in_batches(keys, download_object)

        return len(keys)
