"""Run with `uv run pytest tests/unit/api/test_benchmark_storage.py`.

Cover run-scoped output download URLs and agent-version promotion.
"""

from collections.abc import AsyncIterator
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

import tracker.api.benchmark_storage as benchmark_storage_module
from main import app
from tests.factories import make_benchmark
from tracker.database.models import Benchmark

_client = TestClient(app)


@pytest.fixture
def stored_benchmark(database_session: Session) -> Benchmark:
    benchmark = make_benchmark()
    database_session.add(benchmark)
    database_session.commit()

    return benchmark


def _mock_listing(monkeypatch: pytest.MonkeyPatch, keys: list[str]) -> list[str]:
    """Replace the S3 listing with fixed keys and record the requested prefixes."""
    requested_prefixes: list[str] = []

    async def list_objects(prefix: str, _runtime: object) -> AsyncIterator[str]:
        requested_prefixes.append(prefix)
        for key in keys:
            yield key

    monkeypatch.setattr(benchmark_storage_module, "list_s3_objects", list_objects)

    return requested_prefixes


class TestOutputStorage:
    """Output-key listing and just-in-time URL signing for one run."""

    @pytest.mark.parametrize(
        ("subpath", "expected_prefix_suffix"),
        [
            ("", "/"),
            ("task-a/", "/task-a/"),
            ("summary.json", "/summary.json"),
        ],
    )
    def test_prefix_is_slash_bounded_for_directories_and_exact_for_files(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stored_benchmark: Benchmark,
        harness_headers: dict[str, str],
        subpath: str,
        expected_prefix_suffix: str,
    ) -> None:
        """Directory subpaths list a slash-terminated prefix; file subpaths list the exact key."""
        expected_prefix = f"benchmarks/{stored_benchmark.id}{expected_prefix_suffix}"
        key = f"benchmarks/{stored_benchmark.id}/task-a/output.json"
        requested_prefixes = _mock_listing(monkeypatch, [key])
        params = {"subpath": subpath} if subpath else {}
        response = _client.get(
            f"/benchmarks/{stored_benchmark.id}/output-keys",
            params=params,
            headers=harness_headers,
        )

        assert response.status_code == 200
        assert requested_prefixes == [expected_prefix]

        payload = response.json()
        assert payload["prefix"] == expected_prefix
        assert payload["keys"] == [key]

    def test_empty_listing_returns_not_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stored_benchmark: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        _mock_listing(monkeypatch, [])

        response = _client.get(f"/benchmarks/{stored_benchmark.id}/output-keys", headers=harness_headers)

        assert response.status_code == 404
        assert "No files found" in response.json()["detail"]

    def test_signs_only_keys_owned_by_the_requested_run(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stored_benchmark: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        key = f"benchmarks/{stored_benchmark.id}/task-a/output.json"
        presign = AsyncMock(return_value=["https://example.test/file"])
        monkeypatch.setattr(benchmark_storage_module, "create_presigned_urls", presign)

        response = _client.post(
            f"/benchmarks/{stored_benchmark.id}/output-urls",
            json={"keys": [key]},
            headers=harness_headers,
        )

        assert response.status_code == 200
        assert response.json()["files"] == [{"key": key, "download_url": "https://example.test/file"}]

        rejected_response = _client.post(
            f"/benchmarks/{stored_benchmark.id}/output-urls",
            json={"keys": ["benchmarks/another-run/output.json"]},
            headers=harness_headers,
        )
        assert rejected_response.status_code == 400
        presign.assert_awaited_once()


class TestAgentVersionPromotion:
    """Server-side copy of the latest pushed agent onto a run's frozen copy."""

    def test_copies_latest_agent_onto_run_snapshot(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stored_benchmark: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        exists = AsyncMock(return_value=True)
        copy = AsyncMock(return_value=None)
        monkeypatch.setattr(benchmark_storage_module, "s3_object_exists", exists)
        monkeypatch.setattr(benchmark_storage_module, "copy_s3_object", copy)

        response = _client.post(
            f"/benchmarks/{stored_benchmark.id}/agent-version",
            json={"agent_name": "demo"},
            headers=harness_headers,
        )

        assert response.status_code == 204
        copy.assert_awaited_once()
        assert copy.await_args is not None
        assert copy.await_args.args[0] == "agents/demo.zip"
        assert copy.await_args.args[1] == f"benchmarks/{stored_benchmark.id}/demo.zip"

    def test_missing_source_agent_returns_not_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stored_benchmark: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        exists = AsyncMock(return_value=False)
        copy = AsyncMock(return_value=None)
        monkeypatch.setattr(benchmark_storage_module, "s3_object_exists", exists)
        monkeypatch.setattr(benchmark_storage_module, "copy_s3_object", copy)

        response = _client.post(
            f"/benchmarks/{stored_benchmark.id}/agent-version",
            json={"agent_name": "missing"},
            headers=harness_headers,
        )

        assert response.status_code == 404
        copy.assert_not_awaited()

    def test_invalid_agent_name_is_rejected(
        self,
        stored_benchmark: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        response = _client.post(
            f"/benchmarks/{stored_benchmark.id}/agent-version",
            json={"agent_name": "../escape"},
            headers=harness_headers,
        )

        assert response.status_code == 400
