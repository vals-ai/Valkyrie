"""Tests for tracker-mediated agent storage.

Run: uv run pytest tests/unit/cli/agent/test_remote_storage.py

Covers the keyless data path: presigned upload/download round trips, metadata
operations, and run-output downloads through Tracker-issued URLs.
"""

import io
import json
import zipfile
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from tracker.exceptions import S3Error

from valkyrie.cli.agent import remote_storage

_TRACKER_URL = "http://tracker.test"
_S3_URL = "https://bucket.s3.test"


class MockStorageBackend:
    """Emulate the tracker storage endpoints and presigned S3 URLs in one transport."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.promoted: list[dict[str, str]] = []

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path

        if str(request.url).startswith(_S3_URL):
            key = path.lstrip("/")
            if request.method == "PUT":
                self.objects[key] = request.read()
                return httpx.Response(200)
            if key not in self.objects:
                return httpx.Response(404)
            return httpx.Response(200, content=self.objects[key])

        if request.method == "POST" and path.endswith("/upload-url"):
            name = path.split("/")[2]
            return httpx.Response(
                200,
                json={"name": name, "upload_url": f"{_S3_URL}/agents/{name}.zip", "expires_in": 300},
            )
        if request.method == "GET" and path.endswith("/download-url"):
            name = path.split("/")[2]
            if f"agents/{name}.zip" not in self.objects:
                return httpx.Response(404, json={"detail": f"Agent '{name}' not found in S3"})
            return httpx.Response(
                200,
                json={"name": name, "download_url": f"{_S3_URL}/agents/{name}.zip", "expires_in": 300},
            )
        if request.method == "GET" and path == "/agents":
            return httpx.Response(
                200,
                json={
                    "agents": [
                        {"name": Path(key).stem, "last_modified": "2026-01-02 00:00:00+00:00"}
                        for key in sorted(self.objects)
                        if key.startswith("agents/")
                    ]
                },
            )
        if request.method == "DELETE" and path.startswith("/agents/"):
            key = f"agents/{path.split('/')[2]}.zip"
            if key not in self.objects:
                return httpx.Response(404, json={"detail": "not found"})
            del self.objects[key]
            return httpx.Response(204)
        if request.method == "POST" and path.endswith("/agent-version"):
            self.promoted.append({"benchmark_id": path.split("/")[2], "body": request.read().decode()})
            return httpx.Response(204)
        if request.method == "GET" and path.endswith("/output-urls"):
            benchmark_id = path.split("/")[2]
            prefix = f"benchmarks/{benchmark_id}"
            subpath = request.url.params.get("subpath")
            if subpath:
                prefix = f"{prefix}/{subpath.strip('/')}"
            keys = [key for key in sorted(self.objects) if key.startswith(prefix)]
            if not keys:
                return httpx.Response(404, json={"detail": f"No files found under '{prefix}'"})
            return httpx.Response(
                200,
                json={
                    "prefix": prefix,
                    "files": [{"key": key, "download_url": f"{_S3_URL}/{key}"} for key in keys],
                    "expires_in": 300,
                },
            )

        return httpx.Response(404, json={"detail": f"unhandled: {request.method} {path}"})


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> MockStorageBackend:
    mock_backend = MockStorageBackend()
    monkeypatch.setattr(
        remote_storage,
        "_client",
        lambda: httpx.AsyncClient(transport=mock_backend.transport(), base_url=_TRACKER_URL),
    )

    return mock_backend


def _agent_zip_bytes(agent_name: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zip_file:
        zip_file.writestr(f"{agent_name}/contract.yaml", "name: demo\n")

    return buffer.getvalue()


async def test_push_uploads_zip_to_presigned_url(
    backend: MockStorageBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    archive = _agent_zip_bytes("demo")

    def mock_zip_stream(**_kwargs: Any) -> nullcontext[io.BytesIO]:
        return nullcontext(io.BytesIO(archive))

    monkeypatch.setattr(remote_storage, "get_agent_zip_stream", mock_zip_stream)

    await remote_storage.push_agent_remote("demo", Path("/unused"))

    assert backend.objects["agents/demo.zip"] == archive


async def test_download_returns_stored_zip_and_missing_raises(backend: MockStorageBackend) -> None:
    backend.objects["agents/demo.zip"] = b"zip-bytes"

    assert await remote_storage.download_agent_zip_remote("demo") == b"zip-bytes"

    with pytest.raises(S3Error, match="Agent 'missing' not found in S3"):
        await remote_storage.download_agent_zip_remote("missing")


async def test_list_parses_names_and_timestamps(backend: MockStorageBackend) -> None:
    backend.objects["agents/demo.zip"] = b"zip-bytes"

    assert await remote_storage.list_agents_remote() == [("demo", datetime.fromisoformat("2026-01-02 00:00:00+00:00"))]


async def test_remove_deletes_and_missing_raises(backend: MockStorageBackend) -> None:
    backend.objects["agents/demo.zip"] = b"zip-bytes"

    await remote_storage.remove_agent_remote("demo")
    assert backend.objects == {}

    with pytest.raises(S3Error, match="Agent 'demo' could not be found"):
        await remote_storage.remove_agent_remote("demo")


async def test_agent_version_promotion_posts_agent_name(backend: MockStorageBackend) -> None:
    await remote_storage.update_benchmark_agent_version_remote("demo", "benchmark-1")

    assert backend.promoted[0]["benchmark_id"] == "benchmark-1"

    body = json.loads(backend.promoted[0]["body"])
    assert body == {"agent_name": "demo"}


async def test_download_outputs_writes_relative_layout(
    backend: MockStorageBackend,
    tmp_path: Path,
) -> None:
    backend.objects["benchmarks/run-1/task-a/output.json"] = b"{}"
    backend.objects["benchmarks/run-1/summary.json"] = b"[]"

    count = await remote_storage.download_outputs_remote("run-1", "", tmp_path)

    assert count == 2
    assert (tmp_path / "task-a" / "output.json").read_bytes() == b"{}"
    assert (tmp_path / "summary.json").read_bytes() == b"[]"


async def test_download_outputs_subpath_scopes_the_listing(
    backend: MockStorageBackend,
    tmp_path: Path,
) -> None:
    backend.objects["benchmarks/run-1/task-a/output.json"] = b"{}"
    backend.objects["benchmarks/run-1/task-b/output.json"] = b"{}"

    count = await remote_storage.download_outputs_remote("run-1", "task-a", tmp_path)

    assert count == 1
    assert (tmp_path / "output.json").exists()
    assert not (tmp_path / "task-b").exists()
