"""Tests for run artifact downloads from S3.

Run: uv run pytest tests/unit/cli/run/test_artifacts.py
"""

import asyncio
from pathlib import Path
from types import TracebackType
from typing import Any

import click
import pytest
from tracker.exceptions import S3Error

from valkyrie.cli.run.artifacts import download_s3_path


class MockBody:
    """Track concurrent reads of one S3 response body."""

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


class MockS3Client:
    """Serve deterministic S3 pages and response bodies."""

    def __init__(self, payloads: dict[str, bytes], tracker: ConcurrencyTracker, expected_prefix: str) -> None:
        self._payloads = payloads
        self._tracker = tracker
        self._expected_prefix = expected_prefix

    def client(self, _name: str) -> "MockS3Client":
        return self

    async def __aenter__(self) -> "MockS3Client":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    def get_paginator(self, _name: str) -> "MockS3Client":
        return self

    async def paginate(self, *, Bucket: str, Prefix: str) -> Any:
        assert Bucket == "test-bucket"
        assert Prefix == self._expected_prefix
        yield {"Contents": [{"Key": key} for key in self._payloads]}

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, MockBody]:
        assert Bucket == "test-bucket"
        return {"Body": MockBody(self._payloads[Key], self._tracker)}


def patch_s3(
    monkeypatch: pytest.MonkeyPatch,
    payloads: dict[str, bytes],
    tracker: ConcurrencyTracker,
    expected_prefix: str,
) -> None:
    monkeypatch.setattr("valkyrie.cli.run.artifacts.cli_s3.fetch_bucket_name", lambda: "test-bucket")
    monkeypatch.setattr(
        "valkyrie.cli.run.artifacts.cli_s3.s3_client",
        lambda: MockS3Client(payloads, tracker, expected_prefix),
    )


async def test_download_s3_path_downloads_files_in_parallel(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    tracker = ConcurrencyTracker()
    payloads = {
        "benchmarks/run-1/task-a/output.json": b"a",
        "benchmarks/run-1/task-b/output.json": b"b",
        "benchmarks/run-1/summary.json": b"summary",
    }

    patch_s3(monkeypatch, payloads, tracker, "benchmarks/run-1/")

    count = await download_s3_path("benchmarks/run-1", tmp_path)

    assert count == 3
    assert (tmp_path / "task-a" / "output.json").read_bytes() == b"a"
    assert (tmp_path / "task-b" / "output.json").read_bytes() == b"b"
    assert (tmp_path / "summary.json").read_bytes() == b"summary"
    assert tracker.max_active > 1


async def test_download_s3_path_handles_exact_file_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    tracker = ConcurrencyTracker()
    payloads = {"benchmarks/run-1/results.json": b"results"}

    patch_s3(monkeypatch, payloads, tracker, "benchmarks/run-1/results.json")

    count = await download_s3_path("benchmarks/run-1/results.json", tmp_path)

    assert count == 1
    assert (tmp_path / "results.json").read_bytes() == b"results"


async def test_download_s3_path_rejects_keys_outside_output_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """S3 object keys must not write outside the requested output directory.

    Test cases:
    - A key containing a parent-directory segment raises an S3 error.
    - No file is written beside the output directory.
    """
    tracker = ConcurrencyTracker()
    payloads = {"benchmarks/run-1/../escaped.txt": b"escaped"}
    output_dir = tmp_path / "output"
    patch_s3(monkeypatch, payloads, tracker, "benchmarks/run-1/")

    with pytest.raises(S3Error, match="Requested path is not relative the output directory"):
        await download_s3_path("benchmarks/run-1", output_dir)

    assert not (tmp_path / "escaped.txt").exists()


async def test_download_s3_path_requires_configured_bucket(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """S3 artifact downloads should fail with the CLI config error before making AWS calls.

    Test cases:
    - Missing S3_BUCKET raises the same ClickException surfaced by CLI commands.
    """
    monkeypatch.setattr(
        "valkyrie.cli.s3_config.load_config",
        lambda: {
            "AWS_ACCESS_KEY_ID": "key",
            "AWS_SECRET_ACCESS_KEY": "secret",
            "AWS_DEFAULT_REGION": "us-east-1",
        },
    )

    with pytest.raises(click.ClickException, match="S3_BUCKET key not found"):
        await download_s3_path("benchmarks/run-1", tmp_path)
