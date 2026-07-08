import asyncio
from pathlib import Path
from types import TracebackType
from typing import Any

import pytest

from valkyrie.cli.run.artifacts import download_s3_path


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
    monkeypatch.setattr("valkyrie.cli.run.artifacts._fetch_bucket_name", lambda: "test-bucket")
    monkeypatch.setattr("valkyrie.cli.run.artifacts._s3_client", lambda: FakeS3Client(payloads, tracker))


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
