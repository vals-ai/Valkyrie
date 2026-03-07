import asyncio
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import aioboto3
import click
from botocore.exceptions import ClientError
from tracker import handle_s3_error
from tracker.database.models import AgentContractRequest
from tracker.exceptions import S3Error

from agentic_harness.cli.bundler import get_agent_zip_stream, get_contract_from_zip_bytes
from agentic_harness.schemas import AgentConfig


def _fetch_bucket_name() -> str:
    from agentic_harness.cli.utils import load_config

    config = load_config()
    bucket_name = config.get("S3_BUCKET")
    if not bucket_name:
        raise click.ClickException("S3_BUCKET key not found. Add it using 'harness config modify' first.")

    return bucket_name


async def install_agent(agent_name: str | None, github_url: str):
    """Clone a GitHub repository and install it as an agent to S3"""
    from agentic_harness.cli.utils import run_with_spinner

    # If agent_name is not provided, extract from github_url
    if agent_name is None:
        agent_name = github_url.rstrip("/").split("/")[-1].replace(".git", "")

    # Clone the repo to a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "temp_repo"

        async def clone_repo() -> None:
            """Clone the repository."""
            process = await asyncio.create_subprocess_exec(
                "git",
                "clone",
                github_url,
                str(temp_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            _, stderr = await process.communicate()

            if process.returncode != 0:
                error_message = stderr.decode() if stderr else "No stderr returned"
                raise RuntimeError(f"Failed to clone repository from {github_url}: {error_message}")

        # Clone with spinner
        await run_with_spinner(clone_repo(), f"Installing agent from {github_url}")

        # Push the agent to S3
        click.echo("Preparing upload...", nl=False)
        await push_agent(agent_name, temp_path)


@handle_s3_error(message="Failed to push agent to S3")
async def push_agent(agent_name: str | None, agent_path: Path):
    """Zip and push an agent to S3 at agents/{agent_name}.zip"""

    # fetch bucket name from config
    bucket_name = _fetch_bucket_name()

    # If agent_name is not provided, use the directory name
    if agent_name is None:
        agent_name = agent_path.name

    with get_agent_zip_stream(agent_name=agent_name, agent_path=agent_path) as file_stream:
        # Get file size for progress bar
        file_stream.seek(0, 2)  # Seek to end
        file_size = file_stream.tell()
        file_stream.seek(0)  # Seek back to start

        async with aioboto3.Session().client("s3") as s3_client:
            # Initiate multipart upload
            key = f"agents/{agent_name}.zip"
            now = datetime.now(timezone.utc).isoformat()

            multipart = await s3_client.create_multipart_upload(
                Bucket=bucket_name,
                Key=key,
                Metadata={"uploaded_at": now},
            )
            upload_id = multipart["UploadId"]

            try:
                # Upload parts with progress tracking
                chunk_size = 5 * 1024 * 1024  # 5MB chunks (S3 minimum for multipart)
                parts: list[dict[str, int | str]] = []
                part_number = 1
                bytes_uploaded = 0

                while True:
                    chunk = file_stream.read(chunk_size)
                    if not chunk:
                        break

                    response = await s3_client.upload_part(
                        Bucket=bucket_name,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk,
                    )

                    parts.append({"ETag": response["ETag"], "PartNumber": part_number})
                    bytes_uploaded += len(chunk)
                    part_number += 1

                    # Show progress bar
                    progress_pct = (bytes_uploaded / file_size * 100) if file_size > 0 else 0
                    bar_width = 30
                    filled_width = int(bar_width * progress_pct / 100)
                    bar = "█" * filled_width + "░" * (bar_width - filled_width)
                    click.echo(f"\rUploading agent  [{bar}]  {progress_pct:.1f}%", nl=False)

                click.echo()

                # Complete the multipart upload
                await s3_client.complete_multipart_upload(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )
            except Exception:
                # Abort the upload on error
                await s3_client.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=key,
                    UploadId=upload_id,
                )
                raise


@handle_s3_error(message="Failed to remove agent from S3")
async def remove_agent(agent_name: str):
    """Remove an agent from S3. Raises an error if the agent doesn't exist"""

    # fetch bucket name from config
    bucket_name = _fetch_bucket_name()

    async with aioboto3.Session().client("s3") as s3_client:
        key = f"agents/{agent_name}.zip"

        try:
            # Check if agent exists and raise if we cannot find it
            await s3_client.head_object(Bucket=bucket_name, Key=key)

            # Remove the agent if it exists
            await s3_client.delete_object(Bucket=bucket_name, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise S3Error(f"Agent '{agent_name}' could not be found.")
            raise


async def list_agents():
    """List all agents in the S3 bucket's agents/ folder with the dates that they were added"""

    # fetch bucket name from config
    bucket_name = _fetch_bucket_name()

    click.echo(f"\r\033[KListing agents from bucket '{bucket_name}'...", nl=False)

    async with aioboto3.Session().client("s3") as s3_client:
        response = await s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix="agents/",
        )

        agents: list[tuple[str, datetime]] = []
        if "Contents" in response:
            for obj in response["Contents"]:
                # Extract agent name from a value like "agents/agent_name.zip"
                key = obj["Key"]
                match = re.match(r"agents/(.+?)\.zip$", cast(str, key))
                if not match:
                    continue

                agent_name = match.group(1)
                last_modified = cast(datetime, obj["LastModified"])
                agents.append((agent_name, last_modified))

        return agents


@handle_s3_error(message="Failed to download agent from S3")
async def download_agent(agent_name: str, output_dir: Path | None) -> None:
    """Download an agent zip from S3, extract it, and show progress"""
    bucket_name = _fetch_bucket_name()

    async with aioboto3.Session().client("s3") as s3_client:
        try:
            response = await s3_client.get_object(Bucket=bucket_name, Key=f"agents/{agent_name}.zip")
            zip_bytes: bytes = cast(bytes, await response["Body"].read())
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                raise S3Error(f"Agent '{agent_name}' not found in S3.")
            raise

    # Extract to the specified output directory or current directory
    extract_dir = Path(output_dir) if output_dir else Path.cwd()
    extract_dir.mkdir(parents=True, exist_ok=True)

    # Extract zip with progress bar
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_file:
        tmp_path = Path(tmp_file.name)
        tmp_path.write_bytes(zip_bytes)

    try:
        with zipfile.ZipFile(tmp_path, "r") as zip_ref:
            file_list = zip_ref.namelist()
            total_files = len(file_list)

            for idx, file_name in enumerate(file_list, 1):
                zip_ref.extract(file_name, extract_dir)

                # Show progress bar
                progress_pct = (idx / total_files * 100) if total_files > 0 else 0
                bar_width = 30
                filled_width = int(bar_width * progress_pct / 100)
                bar = "█" * filled_width + "░" * (bar_width - filled_width)
                click.echo(f"\rExtracting agent [{bar}] {progress_pct:.1f}%", nl=False)

            click.echo()
    finally:
        # Clean up temporary zip file
        tmp_path.unlink()


async def get_contract_from_s3(agent_name: str, agent_config: AgentConfig) -> AgentContractRequest:
    """Download agent zip from S3 and extract contract.py into a temp dir, returning the contract request"""
    bucket_name = _fetch_bucket_name()

    async with aioboto3.Session().client("s3") as s3_client:
        try:
            response = await s3_client.get_object(Bucket=bucket_name, Key=f"agents/{agent_name}.zip")
            zip_bytes: bytes = await response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                raise S3Error(f"Agent '{agent_name}' not found in S3.")
            raise

    return get_contract_from_zip_bytes(agent_name, zip_bytes, agent_config)  # type: ignore
