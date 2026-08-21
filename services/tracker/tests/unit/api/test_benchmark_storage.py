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


class TestOutputURLs:
    """Presigned output-download listing for one run."""

    def test_signs_every_listed_key_under_the_run_prefix(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stored_benchmark: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        """Output URLs must cover exactly the run's listed keys, honoring the subpath filter.

        Test cases:
        - Every listed key receives a presigned URL under the run's prefix.
        - A subpath narrows the listed prefix.
        - An empty listing returns 404 instead of an empty map.
        """
        key = f"benchmarks/{stored_benchmark.id}/task-a/output.json"
        requested_prefixes = _mock_listing(monkeypatch, [key])
        presign = AsyncMock(return_value="https://example.test/file")
        monkeypatch.setattr(benchmark_storage_module, "create_presigned_url", presign)

        response = _client.get(f"/benchmarks/{stored_benchmark.id}/output-urls", headers=harness_headers)

        assert response.status_code == 200

        payload = response.json()
        assert payload["prefix"] == f"benchmarks/{stored_benchmark.id}"
        assert payload["files"] == [{"key": key, "download_url": "https://example.test/file"}]

        subpath_response = _client.get(
            f"/benchmarks/{stored_benchmark.id}/output-urls",
            params={"subpath": "task-a/"},
            headers=harness_headers,
        )
        assert subpath_response.status_code == 200
        assert requested_prefixes[-1] == f"benchmarks/{stored_benchmark.id}/task-a"

    def test_empty_listing_returns_not_found(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stored_benchmark: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        _mock_listing(monkeypatch, [])

        response = _client.get(f"/benchmarks/{stored_benchmark.id}/output-urls", headers=harness_headers)

        assert response.status_code == 404
        assert "No files found" in response.json()["detail"]


class TestAgentVersionPromotion:
    """Server-side copy of the latest pushed agent onto a run's frozen copy."""

    def test_copies_latest_agent_onto_run_snapshot(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stored_benchmark: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        """Promotion must copy agents/<name>.zip to the run's frozen key.

        Test cases:
        - An existing agent is copied to the run-scoped destination key.
        - A missing source agent returns 404 without copying.
        - An invalid agent name is rejected before storage work.
        """
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
