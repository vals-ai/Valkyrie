import asyncio
import io
import re
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Coroutine, TypeVar, cast

import aioboto3
import click
import yaml
from botocore.exceptions import ClientError
from tracker import handle_s3_error
from tracker.aws.s3 import (
    copy_s3_object,
    get_benchmark_contract_s3_key,
    get_contract_s3_key,
    s3_object_exists,
)
from tracker.aws.s3 import list_agents as list_s3_agents
from tracker.database.models import AgentContractRequest
from tracker.exceptions import S3Error
from tracker.types import AWSCredentials

from valkyrie.cli.bundler import get_agent_zip_stream, get_contract_from_zip_bytes, read_agent_name
from valkyrie.cli.config.state import load_config
from valkyrie.schemas import AgentConfig, validate_agent_name

T = TypeVar("T")


async def run_with_spinner(coro: Coroutine[Any, Any, T], message: str) -> T:
    """Run an async coroutine with an animated spinner."""
    spinner_frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    frame_index = 0

    max_width = shutil.get_terminal_size().columns - 2
    display_message = message if len(message) <= max_width else message[: max_width - 1] + "…"

    async def show_spinner() -> None:
        nonlocal frame_index
        while not task.done():
            frame = spinner_frames[frame_index % len(spinner_frames)]
            click.echo(f"\r{frame} {display_message}", nl=False)
            frame_index += 1
            await asyncio.sleep(0.1)
        click.echo("\r\033[K", nl=False)

    task = asyncio.create_task(coro)
    spinner_task = asyncio.create_task(show_spinner())

    try:
        result = await task
    finally:
        spinner_task.cancel()
        try:
            await spinner_task
        except asyncio.CancelledError:
            click.echo("\r\033[K", nl=False)

    return result


def _fetch_bucket_name() -> str:
    config = load_config()
    bucket_name = config.get("S3_BUCKET")
    if not bucket_name:
        raise click.ClickException("S3_BUCKET key not found. Add it using 'valkyrie config set' first.")

    return bucket_name


def _aws_credentials() -> AWSCredentials:
    """Build AWS credentials from the valkyrie config."""
    config = load_config()
    return AWSCredentials(
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
        aws_default_region=config["AWS_DEFAULT_REGION"],
    )


def _s3_client():
    """Create an aioboto3 S3 client using credentials from the valkyrie config."""
    config = load_config()
    session = aioboto3.Session(
        aws_access_key_id=config.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config.get("AWS_SECRET_ACCESS_KEY"),
        region_name=config.get("AWS_DEFAULT_REGION"),
    )
    return session.client("s3")


async def _run_git_command(repo_path: Path | None, *args: str) -> None:
    """Execute a git command by building the command using the provided args"""
    cmd = ["git"]

    # Run git command without needing to be in the directory first
    if repo_path:
        cmd.extend(["-C", str(repo_path)])

    cmd.extend(args)

    # Run subprocess, collecting the error using the stderr
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    _, stderr = await process.communicate()

    # If the process error'd out we show the end user and do not proceed
    if process.returncode != 0:
        error_message = stderr.decode() if stderr else "No stderr returned"
        raise RuntimeError(f"Git command failed: {error_message}")


async def install_agent(agent_name: str | None, github_url: str) -> str:
    """Clone a GitHub repository and install it as an agent to S3. Returns the resolved agent name."""

    # Parse the GitHub URL to detect subfolder specification
    # Matches: https://github.com/user/repo or https://github.com/user/repo/tree/branch/path/to/folder
    github_pattern = r"(https://github\.com/[^/]+/[^/]+?)(?:/tree/([^/]+)/(.+?))?(?:\.git)?/?$"
    match = re.match(github_pattern, github_url.rstrip("/"))

    # If user provides option other than a github url NOTE: Only support github, not gitlab
    if not match:
        raise RuntimeError(f"Invalid GitHub URL format: `{github_url}`. Only github is supported")

    base_url = match.group(1)
    branch = match.group(2)
    subfolder = match.group(3)

    # Clone the repo to a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "temp_repo"

        async def clone_repo() -> None:
            """Clone the repository path specified by the user"""
            clone_args = ["clone"]

            # If subfolder we need to only bundle that directory
            if subfolder:
                clone_args.extend(["--no-checkout", "--filter=blob:none"])

            clone_args.extend([base_url, str(temp_path)])

            # Clone the repository using the direct path user specified
            await _run_git_command(None, *clone_args)

            # Setup sparse-checkout if installing a subfolder
            if subfolder:
                await _run_git_command(temp_path, "sparse-checkout", "init", "--cone")
                await _run_git_command(temp_path, "sparse-checkout", "set", subfolder)

            # Checkout specific branch is specified inside of the URL, else we just checkout the head
            checkout_branch = branch or "HEAD"
            await _run_git_command(temp_path, "checkout", checkout_branch)

            # Initialize submodules for the checked-out content only
            await _run_git_command(temp_path, "submodule", "update", "--init", "--recursive")

        # Clone with a loading spinner
        await run_with_spinner(clone_repo(), f"Installing agent from {github_url}")

        # Push the agent to S3 after its installed
        click.echo("Preparing upload...", nl=False)
        agent_path = temp_path / subfolder if subfolder else temp_path

        if subfolder and not agent_path.exists():
            raise RuntimeError(f"Subfolder '{subfolder}' not found in repository")

        resolved_name = validate_agent_name(agent_name) if agent_name else read_agent_name(agent_path)
        await push_agent(resolved_name, agent_path)

    return resolved_name


@handle_s3_error(message="Failed to push agent to S3")
async def push_agent(agent_name: str, agent_path: Path):
    """Zip and push an agent to S3 at agents/{agent_name}.zip"""

    # fetch bucket name from config
    bucket_name = _fetch_bucket_name()

    with get_agent_zip_stream(agent_name=agent_name, agent_path=agent_path) as file_stream:
        # Get file size for progress bar
        file_stream.seek(0, 2)  # Seek to end
        file_size = file_stream.tell()
        file_stream.seek(0)  # Seek back to start

        async with _s3_client() as s3_client:
            # Initiate multipart upload
            key = get_contract_s3_key(agent_name)
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


async def update_benchmark_agent_version(agent_name: str, benchmark_id: str) -> None:
    """Overwrite the frozen benchmark agent copy from agents/<name>.zip in S3."""
    aws = _aws_credentials()
    bucket_name = _fetch_bucket_name()
    source_key = get_contract_s3_key(agent_name)
    dest_key = get_benchmark_contract_s3_key(benchmark_id, agent_name)

    if not await s3_object_exists(source_key, aws, bucket_name):
        raise S3Error(f"Agent '{agent_name}.zip' not found in S3.")

    await copy_s3_object(source_key, dest_key, aws, bucket_name)


@handle_s3_error(message="Failed to remove agent from S3")
async def remove_agent(agent_name: str):
    """Remove an agent from S3. Raises an error if the agent doesn't exist"""

    # fetch bucket name from config
    bucket_name = _fetch_bucket_name()

    async with _s3_client() as s3_client:
        key = get_contract_s3_key(agent_name)

        try:
            # Check if agent exists and raise if we cannot find it
            await s3_client.head_object(Bucket=bucket_name, Key=key)

            # Remove the agent if it exists
            await s3_client.delete_object(Bucket=bucket_name, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise S3Error(f"Agent '{agent_name}' could not be found.")
            raise


async def list_agents() -> list[tuple[str, datetime | None]]:
    """List all agents in the S3 bucket's agents/ folder with the dates that they were added."""
    bucket_name = _fetch_bucket_name()

    click.echo(f"\r\033[KListing agents from bucket '{bucket_name}'...", nl=False)

    return await list_s3_agents(_aws_credentials(), bucket_name)


@handle_s3_error(message="Failed to download agent from S3")
async def download_agent(agent_name: str, output_dir: Path | None) -> None:
    """Download an agent zip from S3, extract it, and show progress"""
    bucket_name = _fetch_bucket_name()

    async with _s3_client() as s3_client:
        try:
            response = await s3_client.get_object(Bucket=bucket_name, Key=get_contract_s3_key(agent_name))
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


async def get_ingest_lambda_from_s3(agent_name: str) -> str | None:
    """Read just the ``ingest_lambda`` field from the currently-pushed agent contract.

    Resolves to the latest pushed version, ignoring whatever snapshot is stored on a
    benchmark run. This lets ``valk run analyze`` work on past runs after their
    contract is updated to declare an analyzer Lambda.

    Reads the ``ingest_lambda`` field directly from the agent's ``contract.yaml``.
    """
    bucket_name = _fetch_bucket_name()

    async with _s3_client() as s3_client:
        try:
            response = await s3_client.get_object(Bucket=bucket_name, Key=get_contract_s3_key(agent_name))
            zip_bytes: bytes = cast(bytes, await response["Body"].read())
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                raise S3Error(f"Agent '{agent_name}' not found in S3.")
            raise

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()

            for ext in (".yaml", ".yml"):
                member = f"{agent_name}/contract{ext}"
                if member in names:
                    zf.extract(member, tmp_path)
                    with open(tmp_path / member, "r") as f:
                        return cast(dict[str, object], yaml.safe_load(f) or {}).get("ingest_lambda")  # type: ignore[return-value]

    return None


async def get_contract_from_s3(agent_name: str, agent_config: AgentConfig) -> AgentContractRequest:
    """Download agent zip from S3 and extract the contract into a temp dir, returning the contract request"""
    bucket_name = _fetch_bucket_name()

    async with _s3_client() as s3_client:
        try:
            response = await s3_client.get_object(Bucket=bucket_name, Key=get_contract_s3_key(agent_name))
            zip_bytes: bytes = await response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                raise S3Error(f"Agent '{agent_name}' not found in S3.")
            raise

    return get_contract_from_zip_bytes(agent_name, zip_bytes, agent_config)  # type: ignore
