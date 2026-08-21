"""Tracker-mediated storage for keyless configs.

Metadata operations go through Tracker endpoints and bulk data moves over
presigned S3 URLs, so a keyless CLI process never constructs an AWS client.
"""

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO

import click
import httpx
from tracker.agent.bundler import get_agent_zip_stream
from tracker.exceptions import S3Error

from valkyrie.cli import s3_config
from valkyrie.cli.runtime_config import tracker_service_url

_TRANSFER_TIMEOUT_SECONDS = 300
_DOWNLOAD_CONCURRENCY = 8
_UPLOAD_CHUNK_BYTES = 5 * 1024 * 1024
# Presigned single-part PUT; S3 caps one part at 5 GiB.
_MAX_SINGLE_PUT_BYTES = 5 * 1024**3


def use_tracker_storage() -> bool:
    """True when the selected config has no static AWS keys (managed mode)."""
    return not s3_config.load_config().get("AWS_ACCESS_KEY_ID")


def _client() -> httpx.AsyncClient:
    headers: dict[str, str] = {}
    api_key = s3_config.load_config().get("api_key")
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


def _echo_progress(label: str, completed: int, total: int) -> None:
    progress_pct = (completed / total * 100) if total > 0 else 0
    bar_width = 30
    filled_width = int(bar_width * progress_pct / 100)
    bar = "█" * filled_width + "░" * (bar_width - filled_width)
    click.echo(f"\r{label} [{bar}]  {progress_pct:.1f}%", nl=False)


async def _stream_with_progress(file_stream: BinaryIO, file_size: int) -> AsyncIterator[bytes]:
    bytes_sent = 0
    while chunk := file_stream.read(_UPLOAD_CHUNK_BYTES):
        yield chunk
        bytes_sent += len(chunk)
        _echo_progress("Uploading agent ", bytes_sent, file_size)


async def push_agent_remote(agent_name: str, agent_path: Path) -> None:
    """Zip an agent and upload it through a Tracker-issued presigned PUT URL."""
    async with _client() as client:
        response = await client.post(f"/agents/{agent_name}/upload-url")
        _raise_for_status(response, "Requesting upload URL")
        upload_url = response.json()["upload_url"]

        with get_agent_zip_stream(agent_name=agent_name, agent_path=agent_path) as file_stream:
            file_stream.seek(0, 2)
            file_size = file_stream.tell()
            file_stream.seek(0)
            if file_size > _MAX_SINGLE_PUT_BYTES:
                raise S3Error(f"Agent zip is {file_size} bytes, above the {_MAX_SINGLE_PUT_BYTES}-byte upload limit.")

            put_response = await client.put(
                upload_url,
                content=_stream_with_progress(file_stream, file_size),
                headers={"Content-Length": str(file_size)},
            )
        click.echo()
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


async def update_benchmark_agent_version_remote(agent_name: str, benchmark_id: str) -> None:
    """Promote the latest pushed agent onto a run's frozen copy through Tracker."""
    async with _client() as client:
        response = await client.post(f"/benchmarks/{benchmark_id}/agent-version", json={"agent_name": agent_name})
        if response.status_code == 404:
            raise S3Error(f"Agent '{agent_name}.zip' not found in S3.")
        _raise_for_status(response, "Updating benchmark agent version")


async def download_outputs_remote(benchmark_id: str, subpath: str, output_dir: Path) -> int:
    """Download a run's output files via Tracker-issued presigned URLs. Returns file count."""
    async with _client() as client:
        params = {"subpath": subpath} if subpath else {}
        response = await client.get(f"/benchmarks/{benchmark_id}/output-urls", params=params)
        _raise_for_status(response, "Requesting output download URLs")
        payload = response.json()

        prefix = payload["prefix"].rstrip("/") + "/"
        output_dir = output_dir.resolve()
        output_dir.mkdir(parents=True, exist_ok=True)

        async def download_file(entry: dict[str, str]) -> None:
            relative = entry["key"].removeprefix(prefix).lstrip("/")
            destination = (output_dir / relative if relative else output_dir / Path(entry["key"]).name).resolve()
            if not destination.is_relative_to(output_dir):
                raise S3Error(f"Requested path is not relative the output directory '{entry['key']}'")
            destination.parent.mkdir(parents=True, exist_ok=True)

            file_response = await client.get(entry["download_url"])
            _raise_for_status(file_response, f"Downloading '{entry['key']}'")
            destination.write_bytes(file_response.content)

        files = payload["files"]
        for start in range(0, len(files), _DOWNLOAD_CONCURRENCY):
            batch = files[start : start + _DOWNLOAD_CONCURRENCY]
            await asyncio.gather(*(download_file(entry) for entry in batch))

        return len(files)
