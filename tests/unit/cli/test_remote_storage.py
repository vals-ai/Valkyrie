"""Tests for tracker-mediated agent storage.

Run: uv run pytest tests/unit/cli/test_remote_storage.py

Covers the keyless data path: presigned upload/download round trips, metadata
operations, run-output downloads, and credential hygiene on presigned requests.
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

from valkyrie.cli import remote_storage

_TRACKER_URL = "http://tracker.test"
_S3_URL = "https://bucket.s3.test"


class MockStorageBackend:
    """Emulate the tracker storage endpoints and presigned S3 URLs in one transport."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.promoted: list[dict[str, str]] = []
        self.requests: list[httpx.Request] = []
        self.output_prefix: str | None = None
        self.fail_s3_transfers = False

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path

        if str(request.url).startswith(_S3_URL):
            if self.fail_s3_transfers:
                return httpx.Response(403, text="<Error>Request has expired</Error>")
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

            # The prefix rule itself is server behavior with its own tests; here the
            # test supplies the intended prefix instead of re-deriving the rule.
            prefix = self.output_prefix if self.output_prefix is not None else f"benchmarks/{benchmark_id}/"
            keys = [key for key in sorted(self.objects) if key.startswith(prefix)]
            if not keys:
                return httpx.Response(404, json={"detail": f"No files found under '{prefix}'"})
            return httpx.Response(
                200,
                json={
                    "prefix": prefix,
                    "files": [{"key": key, "download_url": f"{_S3_URL}/{key}"} for key in keys],
                    "expires_in": 3600,
                },
            )

        return httpx.Response(404, json={"detail": f"unhandled: {request.method} {path}"})

    def request_for(self, method: str, url_fragment: str) -> httpx.Request:
        matches = [
            request for request in self.requests if request.method == method and url_fragment in str(request.url)
        ]
        assert matches, f"no {method} request matching {url_fragment}"

        return matches[0]


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> MockStorageBackend:
    """Route real _client()/_transfer_client() construction through the mock transport."""
    mock_backend = MockStorageBackend()
    real_async_client = httpx.AsyncClient

    def client_with_mock_transport(**kwargs: Any) -> httpx.AsyncClient:
        return real_async_client(transport=mock_backend.transport(), **kwargs)

    monkeypatch.setattr(remote_storage.httpx, "AsyncClient", client_with_mock_transport)
    monkeypatch.setattr(remote_storage.s3_config, "load_config", lambda: {"api_key": "test-api-key"})
    monkeypatch.setattr(remote_storage, "tracker_service_url", lambda: _TRACKER_URL)

    return mock_backend


def _agent_zip_bytes(agent_name: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zip_file:
        zip_file.writestr(f"{agent_name}/contract.yaml", "name: demo\n")

    return buffer.getvalue()


def _mock_zip_stream(monkeypatch: pytest.MonkeyPatch, archive: bytes) -> None:
    def zip_stream(**_kwargs: Any) -> nullcontext[io.BytesIO]:
        return nullcontext(io.BytesIO(archive))

    monkeypatch.setattr(remote_storage, "get_agent_zip_stream", zip_stream)


async def test_push_uploads_zip_and_keeps_credentials_off_s3(
    backend: MockStorageBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Push must upload the exact zip via a fixed-length PUT that carries no tracker credentials.

    Test cases:
    - The uploaded bytes land under agents/<name>.zip.
    - The PUT sends an explicit Content-Length and no chunked transfer encoding.
    - The tracker call carries X-Api-Key; the presigned S3 call does not.
    """
    archive = _agent_zip_bytes("demo")
    _mock_zip_stream(monkeypatch, archive)

    await remote_storage.push_agent_remote("demo", Path("/unused"))

    assert backend.objects["agents/demo.zip"] == archive

    upload_url_request = backend.request_for("POST", "/agents/demo/upload-url")
    assert upload_url_request.headers["x-api-key"] == "test-api-key"

    put_request = backend.request_for("PUT", f"{_S3_URL}/agents/demo.zip")
    assert put_request.headers["content-length"] == str(len(archive))
    assert "transfer-encoding" not in put_request.headers
    assert "x-api-key" not in put_request.headers


class MockOversizedStream:
    """Report a size above the single-PUT cap without allocating real data."""

    def seek(self, _offset: int, _whence: int = 0) -> None:
        return None

    def tell(self) -> int:
        return remote_storage._MAX_SINGLE_PUT_BYTES + 1


async def test_push_rejects_zip_above_single_put_limit(
    backend: MockStorageBackend,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def zip_stream(**_kwargs: Any) -> nullcontext[MockOversizedStream]:
        return nullcontext(MockOversizedStream())

    monkeypatch.setattr(remote_storage, "get_agent_zip_stream", zip_stream)

    with pytest.raises(S3Error, match="above the .*upload limit"):
        await remote_storage.push_agent_remote("demo", Path("/unused"))

    assert backend.requests == []


async def test_presigned_transfer_failures_surface_as_s3_error(
    backend: MockStorageBackend,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed presigned S3 transfer must surface as a clean S3Error.

    Test cases:
    - Upload PUT failure after a valid upload URL was issued.
    - Agent download GET failure after a valid download URL was issued.
    - Per-file GET failure during a run-output download batch.
    """
    backend.objects["agents/demo.zip"] = b"zip-bytes"
    backend.objects["benchmarks/run-1/summary.json"] = b"[]"
    backend.fail_s3_transfers = True
    _mock_zip_stream(monkeypatch, _agent_zip_bytes("demo"))

    with pytest.raises(S3Error, match=r"Uploading agent failed \(403\)"):
        await remote_storage.push_agent_remote("demo", Path("/unused"))

    with pytest.raises(S3Error, match=r"Downloading agent failed \(403\)"):
        await remote_storage.download_agent_zip_remote("demo")

    with pytest.raises(S3Error, match=r"Downloading 'benchmarks/run-1/summary.json' failed \(403\)"):
        await remote_storage.download_outputs_remote("run-1", "", tmp_path)


async def test_download_returns_stored_zip_and_missing_raises(backend: MockStorageBackend) -> None:
    backend.objects["agents/demo.zip"] = b"zip-bytes"

    assert await remote_storage.download_agent_zip_remote("demo") == b"zip-bytes"

    with pytest.raises(S3Error, match="Agent 'missing' not found in S3"):
        await remote_storage.download_agent_zip_remote("missing")


async def test_download_transfer_carries_no_tracker_credentials(backend: MockStorageBackend) -> None:
    backend.objects["agents/demo.zip"] = b"zip-bytes"

    await remote_storage.download_agent_zip_remote("demo")

    s3_request = backend.request_for("GET", f"{_S3_URL}/agents/demo.zip")
    assert "x-api-key" not in s3_request.headers


async def test_list_parses_names_and_timestamps(backend: MockStorageBackend) -> None:
    backend.objects["agents/demo.zip"] = b"zip-bytes"

    assert await remote_storage.list_agents_remote() == [("demo", datetime.fromisoformat("2026-01-02 00:00:00+00:00"))]


async def test_remove_deletes_and_missing_raises(backend: MockStorageBackend) -> None:
    backend.objects["agents/demo.zip"] = b"zip-bytes"

    await remote_storage.remove_agent_remote("demo")
    assert backend.objects == {}

    with pytest.raises(S3Error, match="not found"):
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


async def test_download_outputs_single_file_subpath_lands_flat(
    backend: MockStorageBackend,
    tmp_path: Path,
) -> None:
    """A suffix-bearing subpath must write the file directly into the output directory."""
    backend.objects["benchmarks/run-1/summary.json"] = b"[]"
    backend.output_prefix = "benchmarks/run-1/summary.json"

    count = await remote_storage.download_outputs_remote("run-1", "summary.json", tmp_path)

    assert count == 1
    assert (tmp_path / "summary.json").read_bytes() == b"[]"


async def test_download_outputs_subpath_scopes_the_listing(
    backend: MockStorageBackend,
    tmp_path: Path,
) -> None:
    backend.objects["benchmarks/run-1/task-a/output.json"] = b"{}"
    backend.objects["benchmarks/run-1/task-b/output.json"] = b"{}"
    backend.output_prefix = "benchmarks/run-1/task-a/"

    count = await remote_storage.download_outputs_remote("run-1", "task-a", tmp_path)

    assert count == 1
    assert (tmp_path / "output.json").exists()
    assert not (tmp_path / "task-b").exists()


async def test_download_outputs_rejects_keys_escaping_output_dir(
    backend: MockStorageBackend,
    tmp_path: Path,
) -> None:
    """A server-returned key must not be able to write outside the output directory."""
    backend.objects["benchmarks/run-1/../../escape.txt"] = b"nope"

    with pytest.raises(S3Error, match="not relative to the output directory"):
        await remote_storage.download_outputs_remote("run-1", "", tmp_path / "out")

    assert not (tmp_path / "escape.txt").exists()


@pytest.mark.parametrize(
    ("response", "expected_message"),
    [
        (
            httpx.Response(500, json={"detail": "storage backend unavailable"}),
            r"Listing agents failed \(500\): storage backend unavailable",
        ),
        (
            httpx.Response(502, text="<html>bad gateway</html>"),
            r"Listing agents failed \(502\): <html>bad gateway</html>",
        ),
    ],
)
def test_raise_for_status_reports_detail_or_body(response: httpx.Response, expected_message: str) -> None:
    with pytest.raises(S3Error, match=expected_message):
        remote_storage._raise_for_status(response, "Listing agents")
