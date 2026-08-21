"""SPIKE (throwaway): tracker-mediated agent storage requiring no local AWS credentials.

Data plane uses presigned S3 URLs issued by Tracker; metadata operations are
Tracker endpoints. The CLI process never constructs an AWS client.
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from tracker.agent.bundler import get_agent_zip_stream
from tracker.exceptions import S3Error

from valkyrie.cli.config.state import read_config_if_exists
from valkyrie.cli.runtime_config import tracker_service_url

_TRANSFER_TIMEOUT_SECONDS = 300


def use_tracker_storage() -> bool:
    """True when the selected config has no static AWS keys (managed mode)."""
    config = read_config_if_exists()
    return not config.get("AWS_ACCESS_KEY_ID")


def _client() -> httpx.AsyncClient:
    headers: dict[str, str] = {}
    api_key = read_config_if_exists().get("api_key")
    if api_key:
        headers["X-Api-Key"] = str(api_key)
    return httpx.AsyncClient(base_url=tracker_service_url(), headers=headers, timeout=_TRANSFER_TIMEOUT_SECONDS)


def _raise_for_status(response: httpx.Response, action: str) -> None:
    if response.status_code >= 400:
        detail: Any = response.text
        try:
            detail = response.json().get("detail", detail)
        except ValueError:
            pass
        raise S3Error(f"{action} failed ({response.status_code}): {detail}")


async def push_agent_remote(agent_name: str, agent_path: Path) -> None:
    """Zip an agent and upload it through a Tracker-issued presigned PUT URL."""
    async with _client() as client:
        response = await client.post(f"/agents/{agent_name}/upload-url")
        _raise_for_status(response, "Requesting upload URL")
        upload_url = response.json()["upload_url"]

        with get_agent_zip_stream(agent_name=agent_name, agent_path=agent_path) as file_stream:
            zip_bytes = file_stream.read()

        # The presigned PUT is signed without a Content-Type, so none may be sent.
        put_response = await client.put(upload_url, content=zip_bytes)
        _raise_for_status(put_response, "Uploading agent")


async def download_agent_zip_remote(agent_name: str) -> bytes:
    """Download an agent zip through a Tracker-issued presigned GET URL."""
    async with _client() as client:
        response = await client.get(f"/agents/{agent_name}/download-url")
        if response.status_code == 404:
            raise S3Error(f"Agent '{agent_name}' not found in S3.")
        _raise_for_status(response, "Requesting download URL")

        download_response = await client.get(response.json()["download_url"])
        _raise_for_status(download_response, "Downloading agent")

        return download_response.content


async def list_agents_remote() -> list[tuple[str, datetime | None]]:
    """List agents through the Tracker agents endpoint."""
    async with _client() as client:
        response = await client.get("/agents")
        _raise_for_status(response, "Listing agents")

        entries = response.json()["agents"]

    return [
        (entry["name"], datetime.fromisoformat(entry["last_modified"]) if entry.get("last_modified") else None)
        for entry in entries
    ]


async def remove_agent_remote(agent_name: str) -> None:
    """Delete an agent through the Tracker agents endpoint."""
    async with _client() as client:
        response = await client.delete(f"/agents/{agent_name}")
        if response.status_code == 404:
            raise S3Error(f"Agent '{agent_name}' could not be found.")
        _raise_for_status(response, "Removing agent")
