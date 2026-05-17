import asyncio
from pathlib import Path
from types import TracebackType

import pytest

from valkyrie.cli.s3_client import download_s3_path


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


class FakePaginator:
    def __init__(self, keys: list[str]) -> None:
        self._keys = keys

    async def paginate(self, **_kwargs: object):
        yield {"Contents": [{"Key": key} for key in self._keys]}


class FakeS3Client:
    def __init__(self, payloads: dict[str, bytes], tracker: ConcurrencyTracker) -> None:
        self._payloads = payloads
        self._tracker = tracker

    def get_paginator(self, _name: str) -> FakePaginator:
        return FakePaginator(list(self._payloads))

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, FakeBody]:
        assert Bucket == "test-bucket"
        return {"Body": FakeBody(self._payloads[Key], self._tracker)}


class FakeS3ClientContext:
    def __init__(self, client: FakeS3Client) -> None:
        self._client = client

    async def __aenter__(self) -> FakeS3Client:
        return self._client

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None


class FakeSession:
    def __init__(self, client: FakeS3Client) -> None:
        self._client = client

    def client(self, _name: str) -> FakeS3ClientContext:
        return FakeS3ClientContext(self._client)


@pytest.mark.asyncio
async def test_download_s3_path_downloads_files_in_parallel(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    tracker = ConcurrencyTracker()
    payloads = {
        "benchmarks/run-1/task-a/output.json": b"a",
        "benchmarks/run-1/task-b/output.json": b"b",
        "benchmarks/run-1/summary.json": b"summary",
    }

    monkeypatch.setattr("valkyrie.cli.s3_client._fetch_bucket_name", lambda: "test-bucket")
    monkeypatch.setattr("valkyrie.cli.s3_client.aioboto3.Session", lambda: FakeSession(FakeS3Client(payloads, tracker)))

    count = await download_s3_path("benchmarks/run-1", tmp_path)

    assert count == 3
    assert (tmp_path / "task-a" / "output.json").read_bytes() == b"a"
    assert (tmp_path / "task-b" / "output.json").read_bytes() == b"b"
    assert (tmp_path / "summary.json").read_bytes() == b"summary"
    assert tracker.max_active > 1


@pytest.mark.asyncio
async def test_download_s3_path_handles_exact_file_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    tracker = ConcurrencyTracker()
    payloads = {"benchmarks/run-1/results.json": b"results"}

    monkeypatch.setattr("valkyrie.cli.s3_client._fetch_bucket_name", lambda: "test-bucket")
    monkeypatch.setattr("valkyrie.cli.s3_client.aioboto3.Session", lambda: FakeSession(FakeS3Client(payloads, tracker)))

    count = await download_s3_path("benchmarks/run-1/results.json", tmp_path)

    assert count == 1
    assert (tmp_path / "results.json").read_bytes() == b"results"
