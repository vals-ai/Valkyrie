import asyncio
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import aioboto3
import click
from tracker import handle_s3_error

from agentic_harness.cli.bundler import get_agent_zip_stream


def _fetch_bucket_name() -> str:
    from agentic_harness.cli.utils import load_config

    config = load_config()
    bucket_name = config.get("S3_BUCKET")
    if not bucket_name:
        raise click.ClickException("S3_BUCKET key not found. Add it using 'harness config modify' first.")

    return bucket_name


async def install_agent(agent_name: str | None, github_url: str):
    """Clone a GitHub repository and install it as an agent to S3"""
    # If agent_name is not provided, extract from github_url
    if agent_name is None:
        agent_name = github_url.rstrip("/").split("/")[-1].replace(".git", "")

    # Clone the repo to a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "temp_repo"

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

        # Push the agent to S3
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
        async with aioboto3.Session().client("s3") as s3_client:
            # Set timestamp in metadata to track when the agent was uploaded
            now = datetime.now(timezone.utc).isoformat()
            await s3_client.put_object(
                Bucket=bucket_name,
                Key=f"agents/{agent_name}.zip",
                Body=file_stream.read(),
                Metadata={"uploaded_at": now},
            )


@handle_s3_error(message="Failed to remove agent from S3")
async def remove_agent(agent_name: str):
    """Remove an agent from S3. Raises an error if the agent doesn't exist"""

    # fetch bucket name from config
    bucket_name = _fetch_bucket_name()

    async with aioboto3.Session().client("s3") as s3_client:
        key = f"agents/{agent_name}.zip"

        # Check if agent exists and raise if we cannot find it
        await s3_client.head_object(Bucket=bucket_name, Key=key)

        # Remove the agent if it exists
        await s3_client.delete_object(Bucket=bucket_name, Key=key)


async def list_agents():
    """List all agents in the S3 bucket's agents/ folder with the dates that they were added"""

    # fetch bucket name from config
    bucket_name = _fetch_bucket_name()

    click.echo(f"Listing agents from bucket '{bucket_name}'...")

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
