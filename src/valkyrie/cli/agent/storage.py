"""Tracker-backed library commands and internal run snapshot storage helpers."""

import io
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import cast

import yaml
from tracker.aws.s3 import (
    download_from_s3,
    get_contract_s3_key,
    s3_object_exists,
)
from tracker.exceptions import S3Error
from valkyrie.sdk import ValkyrieClient
from valkyrie.sdk.errors import ValkyrieAPIError

from valkyrie.cli import s3_config as cli_s3
from valkyrie.cli.runtime_config import config_location, tracker_service_url


async def install_agent(agent_name: str | None, github_url: str) -> str:
    """Clone a GitHub agent and upload it through Tracker."""
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        result = await client.agents.install(github_url, name=agent_name)

    return result.name


async def push_agent(agent_name: str | None, agent_path: Path) -> str:
    """Replace the named library agent through Tracker."""
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        result = await client.agents.push(agent_path, name=agent_name)

    return result.name


async def push_agent_if_absent(agent_name: str, agent_path: Path) -> bool:
    """Atomically create a shared agent alias, returning False on a collision."""
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        try:
            await client.agents.push(agent_path, name=agent_name, overwrite=False)
        except ValkyrieAPIError as error:
            if error.status_code == 409:
                return False
            raise
    return True


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
