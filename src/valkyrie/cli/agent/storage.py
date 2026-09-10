"""Tracker-backed library commands and internal run snapshot storage helpers."""

import io
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import yaml
from botocore.exceptions import ClientError
from tracker import handle_s3_error
from tracker.aws.s3 import (
    copy_s3_object,
    download_from_s3,
    get_benchmark_contract_s3_key,
    get_contract_s3_key,
    s3_object_exists,
)
from tracker.agent.contract import get_contract_from_zip_bytes
from tracker.agent.schemas import AgentConfig
from tracker.database.models import AgentContractRequest
from tracker.exceptions import S3Error
from valkyrie.sdk import ValkyrieClient
from valkyrie.sdk.agent_bundle import get_agent_zip_stream

from valkyrie.cli import s3_config as cli_s3
from valkyrie.cli.runtime_config import config_location, tracker_service_url


async def install_agent(agent_name: str | None, github_url: str) -> str:
    """Clone a GitHub agent and upload it through Tracker."""
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        result = await client.agents.install(github_url, name=agent_name)

    return result.name


async def push_agent(agent_name: str, agent_path: Path) -> None:
    """Replace the named library agent through Tracker."""
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        await client.agents.push(agent_path, name=agent_name)


@handle_s3_error(message="Failed to publish local agent without overwriting an alias")
async def push_agent_if_absent(agent_name: str, agent_path: Path) -> bool:
    """Atomically create a shared agent alias, returning False on a collision."""
    bucket_name = cli_s3.fetch_bucket_name()
    with get_agent_zip_stream(agent_name=agent_name, agent_path=agent_path) as file_stream:
        file_stream.seek(0, 2)
        file_size = file_stream.tell()
        file_stream.seek(0)
        async with cli_s3.s3_client() as client:
            try:
                await client.put_object(
                    Bucket=bucket_name,
                    Key=get_contract_s3_key(agent_name),
                    Body=file_stream,
                    ContentLength=file_size,
                    IfNoneMatch="*",
                    Metadata={"uploaded_at": datetime.now(timezone.utc).isoformat()},
                )
            except ClientError as error:
                code = str(error.response.get("Error", {}).get("Code", ""))
                status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if code in {"PreconditionFailed", "412"} or status == 412:
                    return False
                raise
    return True


async def update_benchmark_agent_version(agent_name: str, benchmark_id: str) -> None:
    """Overwrite the frozen benchmark agent copy from agents/<name>.zip in S3."""
    runtime = cli_s3.aws_runtime()
    source_key = get_contract_s3_key(agent_name)
    dest_key = get_benchmark_contract_s3_key(benchmark_id, agent_name)

    if not await s3_object_exists(source_key, runtime):
        raise S3Error(f"Agent '{agent_name}.zip' not found in S3.")

    await copy_s3_object(source_key, dest_key, runtime)


async def _download_agent_zip(agent_name: str) -> bytes:
    runtime = cli_s3.aws_runtime()
    key = get_contract_s3_key(agent_name)

    if not await s3_object_exists(key, runtime):
        raise S3Error(f"Agent '{agent_name}' not found in S3.")

    return await download_from_s3(key, runtime)


async def remove_agent(agent_name: str) -> None:
    """Remove an existing agent through Tracker."""
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        await client.agents.remove(agent_name)


async def list_agents() -> list[tuple[str, datetime | None]]:
    """List shared agents through Tracker."""
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        result = await client.agents.list()

    return [
        (agent.name, datetime.fromisoformat(agent.last_modified) if agent.last_modified else None)
        for agent in result.agents
    ]


async def download_agent(agent_name: str, output_dir: Path | None, *, overwrite: bool = False) -> None:
    """Download and safely extract the named agent through the SDK."""
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        await client.agents.download(agent_name, output_dir, overwrite=overwrite)


async def get_ingest_lambda_from_s3(agent_name: str) -> str | None:
    """Read just the ``ingest_lambda`` field from the currently-pushed agent contract.

    Resolves to the latest pushed version, ignoring whatever snapshot is stored on a
    benchmark run. This lets ``valk run analyze`` work on past runs after their
    contract is updated to declare an analyzer Lambda.

    Reads the ``ingest_lambda`` field directly from the agent's ``contract.yaml``.
    """
    zip_bytes = await _download_agent_zip(agent_name)

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
    zip_bytes = await _download_agent_zip(agent_name)

    return get_contract_from_zip_bytes(agent_name, zip_bytes, agent_config)  # type: ignore
