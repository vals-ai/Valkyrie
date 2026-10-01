"""Tracker-backed library commands and internal run snapshot storage helpers."""

import tempfile
from datetime import datetime
from pathlib import Path

import yaml
from valkyrie.sdk import ValkyrieClient
from valkyrie.sdk.errors import ValkyrieAPIError

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
    """Atomically create an organization agent alias, returning False on a collision."""
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        try:
            await client.agents.push(agent_path, name=agent_name, overwrite=False)
        except ValkyrieAPIError as error:
            if error.status_code == 409:
                return False
            raise
    return True


async def remove_agent(agent_name: str) -> None:
    """Remove an existing agent through Tracker."""
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        await client.agents.remove(agent_name)


async def list_agents() -> list[tuple[str, datetime | None]]:
    """List the organization's agents through Tracker."""
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


async def get_ingest_lambda(agent_name: str) -> str | None:
    """Read just the ``ingest_lambda`` field from the currently-pushed agent contract.

    Resolves to the latest pushed version, ignoring whatever snapshot is stored on a
    benchmark run. This lets ``valk run analyze`` work on past runs after their
    contract is updated to declare an analyzer Lambda.

    Reads the ``ingest_lambda`` field directly from the agent's ``contract.yaml``.
    """
    with tempfile.TemporaryDirectory() as directory:
        async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
            agent_path = await client.agents.download(agent_name, directory)

        for extension in (".yaml", ".yml"):
            contract_path = agent_path / f"contract{extension}"
            if contract_path.is_file():
                contract = yaml.safe_load(contract_path.read_text()) or {}
                value = contract.get("ingest_lambda")

                return value if isinstance(value, str) else None

    return None
