import asyncio
import importlib.util
import io
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import aioboto3
import click
import yaml
from botocore.exceptions import ClientError
from tracker import handle_s3_error
from tracker.database.models import AgentContractRequest
from tracker.exceptions import S3Error

from valkyrie.cli.bundler import get_agent_zip_stream, get_contract_from_zip_bytes
from valkyrie.cli.utils import load_config, run_with_spinner
from valkyrie.schemas import AgentConfig

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

    # Extract agent name from the url if an alias is not provided
    if agent_name is None:
        if subfolder:
            agent_name = subfolder.split("/")[-1]
        else:
            agent_name = base_url.rstrip("/").split("/")[-1].replace(".git", "")

    # Resolve the agent name to a guaranteed str
    if agent_name is None:
        if subfolder:
            resolved_name: str = subfolder.split("/")[-1]
        else:
            resolved_name = base_url.rstrip("/").split("/")[-1].replace(".git", "")
    else:
        resolved_name = agent_name

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

        await push_agent(resolved_name, agent_path)

    return resolved_name


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

        async with _s3_client() as s3_client:
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


async def update_benchmark_agent_version(agent_name: str, benchmark_id: str) -> None:
    """Overwrite the frozen benchmark agent copy from agents/<name>.zip in S3."""
    bucket_name = _fetch_bucket_name()
    source_key = f"agents/{agent_name}.zip"
    dest_key = f"benchmarks/{benchmark_id}/{agent_name}.zip"

    async with _s3_client() as s3_client:
        try:
            await s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={"Bucket": bucket_name, "Key": source_key},
                Key=dest_key,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                raise S3Error(f"Agent '{agent_name}.zip' not found in S3.") from e
            raise S3Error(f"Failed to copy agent '{agent_name}' in S3: {e}") from e


@handle_s3_error(message="Failed to remove agent from S3")
async def remove_agent(agent_name: str):
    """Remove an agent from S3. Raises an error if the agent doesn't exist"""

    # fetch bucket name from config
    bucket_name = _fetch_bucket_name()

    async with _s3_client() as s3_client:
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

    async with _s3_client() as s3_client:
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

    async with _s3_client() as s3_client:
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


async def get_ingest_lambda_from_s3(agent_name: str) -> str | None:
    """Read just the ``ingest_lambda`` field from the currently-pushed agent contract.

    Resolves to the latest pushed version, ignoring whatever snapshot is stored on a
    benchmark run. This lets ``valk run analyze`` work on past runs after their
    contract is updated to declare an analyzer Lambda.

    Supports YAML contracts directly; for Python contracts, instantiates with an
    empty ``AgentConfig`` and reads the ``ingest_lambda`` property without invoking
    ``run_cmd`` (which would require model validation).
    """
    bucket_name = _fetch_bucket_name()

    async with aioboto3.Session().client("s3") as s3_client:
        try:
            response = await s3_client.get_object(Bucket=bucket_name, Key=f"agents/{agent_name}.zip")
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

            # TODO: remove this branch when we migrate off of Python contracts.
            py_member = f"{agent_name}/contract.py"
            if py_member in names:
                zf.extractall(tmp_path)
                contract_path = tmp_path / py_member
                spec = importlib.util.spec_from_file_location("contract", contract_path)
                if not spec or not spec.loader:
                    return None
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                contract_cls = module.contract
                return contract_cls(AgentConfig()).ingest_lambda

    return None


async def get_contract_from_s3(agent_name: str, agent_config: AgentConfig) -> AgentContractRequest:
    """Download agent zip from S3 and extract contract.py into a temp dir, returning the contract request"""
    bucket_name = _fetch_bucket_name()

    async with _s3_client() as s3_client:
        try:
            response = await s3_client.get_object(Bucket=bucket_name, Key=f"agents/{agent_name}.zip")
            zip_bytes: bytes = await response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                raise S3Error(f"Agent '{agent_name}' not found in S3.")
            raise

    return get_contract_from_zip_bytes(agent_name, zip_bytes, agent_config)  # type: ignore
