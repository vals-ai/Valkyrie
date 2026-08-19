"""Run with `uv run pytest tests/unit/api/test_benchmarks_status.py`.

Cover lightweight benchmark status and result-existence routes.
"""

from unittest.mock import AsyncMock
from uuid import UUID, uuid4

from starlette.testclient import TestClient
import pytest
from sqlmodel import Session

from main import app
from tests.utils import TEST_ORG_ID
from tracker.api.parsing import parse_csv
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    Task,
    TaskStatus,
)

_client = TestClient(app)
_RESULTS_RUN_ID = UUID("22222222-2222-4222-8222-222222222222")


class TestBenchmarkStatusQueries:
    """Benchmark status polling and result-existence queries."""

    def test_benchmarks_status_empty_ids_returns_empty_entries(self) -> None:
        """An empty status filter must be a successful no-op for polling clients.

        Test cases:
        - Empty CSV input and the route both return empty results.
        """
        assert parse_csv(" , ", UUID) == []

        response = _client.get("/benchmarks/status?ids=")

        assert response.status_code == 200
        assert response.json() == {"entries": []}

    def test_benchmarks_status_unknown_id_returns_empty_entries(self) -> None:
        """Unknown run IDs must not create placeholder status entries.

        Test cases:
        - A valid but missing UUID returns an empty list.
        """
        response = _client.get(f"/benchmarks/status?ids={uuid4()}")

        assert response.status_code == 200
        assert response.json() == {"entries": []}

    def test_results_exist_checks_results_key(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
        monkeypatch: pytest.MonkeyPatch,
        harness_headers: dict[str, str],
    ) -> None:
        """Result existence must check the canonical results S3 key."""
        example_benchmark_object.id = _RESULTS_RUN_ID
        database_session.add(example_benchmark_object)
        database_session.commit()
        exists_mock = AsyncMock(return_value=True)
        monkeypatch.setattr("main.s3_object_exists", exists_mock)

        response = _client.get(
            f"/check-results-exist?benchmark_id={_RESULTS_RUN_ID}",
            headers=harness_headers,
        )

        assert response.status_code == 200
        assert response.json() == {"exists": True}
        assert exists_mock.await_args is not None
        assert exists_mock.await_args.args[0] == f"benchmarks/{_RESULTS_RUN_ID}/{example_benchmark_object.name}.json"

    def test_benchmarks_status_counts_all_terminal_tasks(
        self,
        database_session: Session,
        contract: AgentContractRequest,
    ) -> None:
        """Status polling must count stopped tasks as complete alongside finished and errored tasks.

        Test cases:
        - Three terminal states contribute to finished progress while an active task does not.
        - Invalid UUID tokens are ignored without hiding the valid benchmark.
        """
        benchmark_id = uuid4()
        database_session.add(
            Benchmark(
                org_id=TEST_ORG_ID,
                id=benchmark_id,
                name="swebench",
                status=BenchmarkStatus.IN_PROGRESS,
                arguments=BenchmarkArguments(contract=contract, concurrency=1),
            )
        )
        database_session.add_all(
            [
                Task(org_id=TEST_ORG_ID, task_id="finished", benchmark=benchmark_id, status=TaskStatus.FINISHED),
                Task(org_id=TEST_ORG_ID, task_id="error", benchmark=benchmark_id, status=TaskStatus.ERROR),
                Task(org_id=TEST_ORG_ID, task_id="stopped", benchmark=benchmark_id, status=TaskStatus.STOPPED),
                Task(org_id=TEST_ORG_ID, task_id="running", benchmark=benchmark_id, status=TaskStatus.IN_PROGRESS),
            ]
        )
        database_session.commit()

        response = _client.get(f"/benchmarks/status?ids={benchmark_id},not-a-uuid")

        assert response.status_code == 200
        assert response.json()["entries"] == [
            {
                "id": str(benchmark_id),
                "status": "IN_PROGRESS",
                "finished_at": None,
                "executor_release_id": None,
                "current_execution_release_id": None,
                "executor_artifact_digest": None,
                "executor_protocol_version": None,
                "total_tasks": 4,
                "finished_tasks": 3,
                "task_state_counts": {"FINISHED": 1, "ERROR": 1, "STOPPED": 1, "IN_PROGRESS": 1},
            }
        ]
