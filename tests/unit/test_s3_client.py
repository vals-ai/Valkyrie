import asyncio
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import Any

import pytest
from botocore.exceptions import ClientError
from tracker.exceptions import S3Error

from valkyrie.cli.s3_client import download_s3_path, list_agents, push_agent


class FakeBody:
    def __init__(self, payload: bytes, tracker: "ConcurrencyTracker") -> None:
        self._payload = payload
        self._tracker = tracker

    async def read(self) -> bytes:
        self._tracker.active += 1
        self._tracker.max_active = max(self._tracker.max_active, self._tracker.active)
        await asyncio.sleep(0)
        self._tracker.active -= 1
        return self._payload


class ConcurrencyTracker:
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0


class FakeS3Client:
    def __init__(self, payloads: dict[str, bytes], tracker: ConcurrencyTracker) -> None:
        self._payloads = payloads
        self._tracker = tracker

    def client(self, _name: str) -> "FakeS3Client":
        return self

    async def __aenter__(self) -> "FakeS3Client":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    def get_paginator(self, _name: str) -> "FakeS3Client":
        return self

    async def paginate(self, **_kwargs: object) -> Any:
        yield {"Contents": [{"Key": key} for key in self._payloads]}

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, FakeBody]:
        assert Bucket == "test-bucket"
        return {"Body": FakeBody(self._payloads[Key], self._tracker)}


def patch_s3(monkeypatch: pytest.MonkeyPatch, payloads: dict[str, bytes], tracker: ConcurrencyTracker) -> None:
    monkeypatch.setattr("valkyrie.cli.s3_client._fetch_bucket_name", lambda: "test-bucket")
    monkeypatch.setattr("valkyrie.cli.s3_client._s3_client", lambda: FakeS3Client(payloads, tracker))


@pytest.mark.asyncio
async def test_download_s3_path_downloads_files_in_parallel(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    tracker = ConcurrencyTracker()
    payloads = {
        "benchmarks/run-1/task-a/output.json": b"a",
        "benchmarks/run-1/task-b/output.json": b"b",
        "benchmarks/run-1/summary.json": b"summary",
    }

    patch_s3(monkeypatch, payloads, tracker)

    count = await download_s3_path("benchmarks/run-1", tmp_path)

    assert count == 3
    assert (tmp_path / "task-a" / "output.json").read_bytes() == b"a"
    assert (tmp_path / "task-b" / "output.json").read_bytes() == b"b"
    assert (tmp_path / "summary.json").read_bytes() == b"summary"
    assert tracker.max_active > 1


@pytest.mark.asyncio
async def test_download_s3_path_handles_exact_file_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    tracker = ConcurrencyTracker()
    payloads = {"benchmarks/run-1/results.json": b"results"}

    patch_s3(monkeypatch, payloads, tracker)

    count = await download_s3_path("benchmarks/run-1/results.json", tmp_path)

    assert count == 1
    assert (tmp_path / "results.json").read_bytes() == b"results"


class FakePushS3Client:
    """Records multipart-upload, head_object, and copy_object calls for push_agent tests."""

    def __init__(self, existing_keys: set[str]) -> None:
        self._existing_keys = existing_keys
        self.copy_calls: list[dict[str, Any]] = []
        self.head_calls: list[str] = []
        self.multipart_starts: list[str] = []

    def client(self, _name: str) -> "FakePushS3Client":
        return self

    async def __aenter__(self) -> "FakePushS3Client":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    async def create_multipart_upload(self, *, Key: str, **_kwargs: Any) -> dict[str, str]:
        self.multipart_starts.append(Key)
        return {"UploadId": "upload-id"}

    async def upload_part(self, **_kwargs: Any) -> dict[str, str]:
        return {"ETag": "etag"}

    async def complete_multipart_upload(self, **_kwargs: Any) -> None:
        return None

    async def abort_multipart_upload(self, **_kwargs: Any) -> None:
        return None

    async def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.head_calls.append(Key)
        if Key in self._existing_keys:
            return {}
        raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")

    async def copy_object(self, *, Bucket: str, CopySource: dict[str, str], Key: str) -> None:
        self.copy_calls.append({"Bucket": Bucket, "CopySource": CopySource, "Key": Key})


def patch_push_s3(monkeypatch: pytest.MonkeyPatch, client: FakePushS3Client) -> None:
    monkeypatch.setattr("valkyrie.cli.s3_client._fetch_bucket_name", lambda: "test-bucket")
    monkeypatch.setattr("valkyrie.cli.s3_client._s3_client", lambda: client)


@pytest.mark.asyncio
async def test_push_agent_with_version_uploads_immutable_copy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """--version uploads the mutable latest key first, then copies it to agents/{name}/{version}.zip."""
    (tmp_path / "agent.py").write_text("print('hi')")
    client = FakePushS3Client(existing_keys=set())
    patch_push_s3(monkeypatch, client)

    await push_agent("my-agent", tmp_path, version="0.5.0")

    assert client.multipart_starts == ["agents/my-agent.zip"]
    assert client.copy_calls == [
        {
            "Bucket": "test-bucket",
            "CopySource": {"Bucket": "test-bucket", "Key": "agents/my-agent.zip"},
            "Key": "agents/my-agent/0.5.0.zip",
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["", "a/b", "..", "."])
async def test_push_agent_rejects_malformed_version(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, version: str
) -> None:
    """A version that is not a single safe path segment is rejected before any S3 mutation."""
    (tmp_path / "agent.py").write_text("print('hi')")
    client = FakePushS3Client(existing_keys=set())
    patch_push_s3(monkeypatch, client)

    with pytest.raises(S3Error, match="Invalid version"):
        await push_agent("my-agent", tmp_path, version=version)

    assert client.multipart_starts == []
    assert client.copy_calls == []


@pytest.mark.asyncio
async def test_push_agent_refuses_overwriting_released_version(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A versioned key that already exists is immutable; the push fails before uploading or copying."""
    (tmp_path / "agent.py").write_text("print('hi')")
    client = FakePushS3Client(existing_keys={"agents/my-agent/0.5.0.zip"})
    patch_push_s3(monkeypatch, client)

    with pytest.raises(S3Error, match="immutable"):
        await push_agent("my-agent", tmp_path, version="0.5.0")

    assert client.head_calls == ["agents/my-agent/0.5.0.zip"]
    assert client.multipart_starts == []
    assert client.copy_calls == []


class FakeListS3Client:
    """Stubs list_objects_v2 for list_agents tests."""

    def __init__(self, keys: list[str]) -> None:
        self._keys = keys

    def client(self, _name: str) -> "FakeListS3Client":
        return self

    async def __aenter__(self) -> "FakeListS3Client":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    async def list_objects_v2(self, **_kwargs: Any) -> dict[str, Any]:
        return {
            "Contents": [{"Key": key, "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc)} for key in self._keys]
        }


@pytest.mark.asyncio
async def test_list_agents_excludes_nested_versioned_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    """Immutable bundles at agents/{name}/{version}.zip are not listed as phantom agents."""
    client = FakeListS3Client(["agents/my-agent.zip", "agents/my-agent/0.5.0.zip"])
    monkeypatch.setattr("valkyrie.cli.s3_client._fetch_bucket_name", lambda: "test-bucket")
    monkeypatch.setattr("valkyrie.cli.s3_client._s3_client", lambda: client)

    agents = await list_agents()

    assert [name for name, _ in agents] == ["my-agent"]
