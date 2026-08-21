"""Tracker-mediated storage for keyless configs.

Metadata operations go through Tracker endpoints and bulk data moves over
presigned S3 URLs, so a keyless CLI process never constructs an AWS client.
"""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, TypeVar, cast

import click
import httpx
from pydantic import BaseModel
from tracker.agent.bundler import get_agent_zip_stream
from tracker.exceptions import S3Error
from tracker.storage_types import (
    AgentDownloadURLResponse,
    AgentsResponse,
    AgentUploadURLResponse,
    BenchmarkOutputURLsResponse,
    OutputURLEntry,
)

from valkyrie.cli import s3_config
from valkyrie.cli.display import create_progress_bar
from valkyrie.cli.runtime_config import tracker_service_url

_TRANSFER_TIMEOUT_SECONDS = 300
_DOWNLOAD_CONCURRENCY = 8
_UPLOAD_CHUNK_BYTES = 5 * 1024 * 1024
# Presigned single-part PUT; S3 caps one part at 5 GiB.
_MAX_SINGLE_PUT_BYTES = 5 * 1024**3


def use_tracker_storage() -> bool:
    """True when the selected config has no static AWS keys (managed mode)."""
    config = s3_config.load_config()
    access_key_configured = "AWS_ACCESS_KEY_ID" in config
    secret_key_configured = "AWS_SECRET_ACCESS_KEY" in config
    if access_key_configured != secret_key_configured:
        raise click.ClickException("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be configured together.")
    if access_key_configured:
        access_key = config["AWS_ACCESS_KEY_ID"]
        secret_key = config["AWS_SECRET_ACCESS_KEY"]
        if (
            not isinstance(access_key, str)
            or not access_key.strip()
            or not isinstance(secret_key, str)
            or not secret_key.strip()
        ):
            raise click.ClickException("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must not be blank.")
        return False
    if "AWS_SESSION_TOKEN" in config:
        raise click.ClickException("AWS_SESSION_TOKEN requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.")
    return True


def _client() -> httpx.AsyncClient:
    """Open a client for Tracker endpoints, carrying the hosted API key."""
    headers: dict[str, str] = {}
    api_key = s3_config.load_config().get("api_key")
    if api_key:
        headers["X-Api-Key"] = str(api_key)
    return httpx.AsyncClient(base_url=tracker_service_url(), headers=headers, timeout=_TRANSFER_TIMEOUT_SECONDS)


def _transfer_client() -> httpx.AsyncClient:
    """Open a bare client for presigned S3 transfers; it must carry no Tracker credentials."""
    return httpx.AsyncClient(timeout=_TRANSFER_TIMEOUT_SECONDS)


def _error_detail(response: httpx.Response) -> str:
    try:
        payload: object = response.json()
    except ValueError:
        return response.text
    if isinstance(payload, dict):
        return str(cast(dict[str, object], payload).get("detail", response.text))

    return response.text


def _raise_for_status(response: httpx.Response, action: str) -> None:
    if response.status_code >= 400:
        message = f"{action} failed ({response.status_code})"
        detail = _error_detail(response)
        raise S3Error(f"{message}: {detail}" if detail else message)


async def _send(request: Awaitable[httpx.Response], action: str) -> httpx.Response:
    """Send one Tracker or presigned request with product-level transport errors."""
    try:
        return await request
    except httpx.HTTPError as exc:
        raise S3Error(f"{action} failed: {exc}. Check your connection and try again.") from exc


ResponseModel = TypeVar("ResponseModel", bound=BaseModel)


def _parse_tracker_response(
    response: httpx.Response,
    response_model: type[ResponseModel],
    action: str,
) -> ResponseModel:
    """Validate a successful Tracker response before using it."""
    try:
        return response_model.model_validate(response.json())
    except ValueError as exc:
        raise S3Error(f"{action} failed: Tracker returned an invalid response.") from exc


Item = TypeVar("Item")


async def gather_in_batches(items: Sequence[Item], worker: Callable[[Item], Awaitable[None]]) -> None:
    """Run `worker` over `items`, at most _DOWNLOAD_CONCURRENCY at a time."""
    for start in range(0, len(items), _DOWNLOAD_CONCURRENCY):
        batch = items[start : start + _DOWNLOAD_CONCURRENCY]
        await asyncio.gather(*(worker(item) for item in batch))


def resolve_download_destination(key: str, prefix: str, output_dir: Path) -> Path:
    """Map an S3 key under `prefix` to a created path inside `output_dir`, rejecting escapes."""
    relative = key.removeprefix(prefix).lstrip("/")
    destination = (output_dir / relative if relative else output_dir / Path(key).name).resolve()
    if not destination.is_relative_to(output_dir):
        raise S3Error(f"Requested path is not relative to the output directory '{key}'")
    destination.parent.mkdir(parents=True, exist_ok=True)

    return destination


def _echo_progress(label: str, completed: int, total: int) -> None:
    bar, progress_pct = create_progress_bar(completed, total)
    click.echo(f"\r{label} [{bar}]  {progress_pct:.1f}%", nl=False)


async def _stream_with_progress(file_stream: BinaryIO, file_size: int) -> AsyncIterator[bytes]:
    bytes_sent = 0
    while chunk := file_stream.read(_UPLOAD_CHUNK_BYTES):
        yield chunk
        bytes_sent += len(chunk)
        _echo_progress("Uploading agent ", bytes_sent, file_size)


async def push_agent_remote(agent_name: str, agent_path: Path) -> None:
    """Zip an agent and upload it through a Tracker-issued presigned PUT URL."""
    with get_agent_zip_stream(agent_name=agent_name, agent_path=agent_path) as file_stream:
        file_stream.seek(0, 2)
        file_size = file_stream.tell()
        file_stream.seek(0)
        if file_size > _MAX_SINGLE_PUT_BYTES:
            raise S3Error(f"Agent zip is {file_size} bytes, above the {_MAX_SINGLE_PUT_BYTES}-byte upload limit.")

        async with _client() as client:
            response = await _send(client.post(f"/agents/{agent_name}/upload-url"), "Requesting upload URL")
            _raise_for_status(response, "Requesting upload URL")
            upload_response = _parse_tracker_response(response, AgentUploadURLResponse, "Requesting upload URL")

        async with _transfer_client() as transfer:
            put_response = await _send(
                transfer.put(
                    upload_response.upload_url,
                    content=_stream_with_progress(file_stream, file_size),
                    headers={"Content-Length": str(file_size)},
                ),
                "Uploading agent",
            )
    click.echo()
    _raise_for_status(put_response, "Uploading agent")


async def download_agent_zip_remote(agent_name: str) -> bytes:
    """Download an agent zip through a Tracker-issued presigned GET URL."""
    async with _client() as client:
        response = await _send(client.get(f"/agents/{agent_name}/download-url"), "Requesting download URL")
        if response.status_code == 404:
            raise S3Error(_error_detail(response) or f"Agent '{agent_name}' not found in S3.")
        _raise_for_status(response, "Requesting download URL")
        download_url_response = _parse_tracker_response(
            response,
            AgentDownloadURLResponse,
            "Requesting download URL",
        )

    async with _transfer_client() as transfer:
        download_response = await _send(transfer.get(download_url_response.download_url), "Downloading agent")
        _raise_for_status(download_response, "Downloading agent")

        return download_response.content


async def list_agents_remote() -> list[tuple[str, datetime | None]]:
    """List agents through the Tracker agents endpoint."""
    async with _client() as client:
        response = await _send(client.get("/agents"), "Listing agents")
        _raise_for_status(response, "Listing agents")
        payload = _parse_tracker_response(response, AgentsResponse, "Listing agents")

    return [
        (entry.name, datetime.fromisoformat(entry.last_modified) if entry.last_modified else None)
        for entry in payload.agents
    ]


async def remove_agent_remote(agent_name: str) -> None:
    """Delete an agent through the Tracker agents endpoint."""
    async with _client() as client:
        response = await _send(client.delete(f"/agents/{agent_name}"), "Removing agent")
        if response.status_code == 404:
            raise S3Error(_error_detail(response) or f"Agent '{agent_name}' could not be found.")
        _raise_for_status(response, "Removing agent")


async def update_benchmark_agent_version_remote(agent_name: str, benchmark_id: str) -> None:
    """Promote the latest pushed agent onto a run's frozen copy through Tracker."""
    async with _client() as client:
        response = await _send(
            client.post(f"/benchmarks/{benchmark_id}/agent-version", json={"agent_name": agent_name}),
            "Updating benchmark agent version",
        )
        _raise_for_status(response, "Updating benchmark agent version")


async def download_outputs_remote(benchmark_id: str, subpath: str, output_dir: Path) -> int:
    """Download a run's output files via Tracker-issued presigned URLs. Returns file count."""
    async with _client() as client:
        params = {"subpath": subpath} if subpath else {}
        response = await _send(
            client.get(f"/benchmarks/{benchmark_id}/output-urls", params=params),
            "Requesting output download URLs",
        )
        _raise_for_status(response, "Requesting output download URLs")
        payload = _parse_tracker_response(
            response,
            BenchmarkOutputURLsResponse,
            "Requesting output download URLs",
        )

    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    async with _transfer_client() as transfer:

        async def download_file(entry: OutputURLEntry) -> None:
            destination = resolve_download_destination(entry.key, payload.prefix, output_dir)

            action = f"Downloading '{entry.key}'"
            file_response = await _send(transfer.get(entry.download_url), action)
            _raise_for_status(file_response, action)
            destination.write_bytes(file_response.content)

        await gather_in_batches(payload.files, download_file)

    return len(payload.files)
