"""Run with `uv run pytest tests/integration/local/api/test_benchmarks_status.py`.

Exercise benchmark status polling through the real app and local database.
"""

from uuid import uuid4

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import BenchmarkStatus, ExecutorRelease


class TestBenchmarksStatus:
    """Bulk benchmark status responses, scoping, and authentication."""

    def test_status_returns_requested_ids(self, client: TestClient, database_session: Session) -> None:
        """Status polling must return current counts for each requested run.

        Test cases:
        - An authenticated request receives entries for its requested benchmark IDs.
        """
        release = ExecutorRelease(
            id="status-release",
            artifact_uri="s3://artifacts/status.pex",
            artifact_digest="a" * 64,
            protocol_version="1",
        )
        running_benchmark = make_benchmark("running", status=BenchmarkStatus.IN_PROGRESS)
        running_benchmark.current_execution_release_id = release.id
        finished_benchmark = make_benchmark("finished", status=BenchmarkStatus.FINISHED)
        database_session.add_all([release, running_benchmark, finished_benchmark])
        database_session.commit()

        response = client.get(
            f"/benchmarks/status?ids={running_benchmark.id},{finished_benchmark.id}",
            headers={"Authorization": "Bearer fake"},
        )

        assert response.status_code == 200, response.text
        response_body = response.json()
        entries = {entry["id"]: entry for entry in response_body["entries"]}
        assert set(entries) == {str(running_benchmark.id), str(finished_benchmark.id)}
        assert entries[str(running_benchmark.id)]["current_execution_release_id"] == release.id
        assert entries[str(finished_benchmark.id)]["current_execution_release_id"] is None

    def test_status_ignores_foreign_ids(self, client: TestClient, database_session: Session) -> None:
        """Status polling must not reveal runs from another organization.

        Test cases:
        - Requested foreign benchmark IDs are omitted from the response.
        """
        benchmark = make_benchmark("own", status=BenchmarkStatus.IN_PROGRESS)
        database_session.add(benchmark)
        database_session.commit()
        foreign_benchmark_id = uuid4()

        response = client.get(
            f"/benchmarks/status?ids={benchmark.id},{foreign_benchmark_id}",
            headers={"Authorization": "Bearer fake"},
        )

        response_body = response.json()
        assert len(response_body["entries"]) == 1
        assert response_body["entries"][0]["id"] == str(benchmark.id)

    def test_status_no_ids_returns_empty(self, client: TestClient) -> None:
        """An empty status request must be a successful no-op.

        Test cases:
        - Omitting benchmark IDs returns an empty entries list.
        """
        response = client.get("/benchmarks/status", headers={"Authorization": "Bearer fake"})

        assert response.status_code == 200
        assert response.json() == {"entries": []}

    def test_status_unauthenticated_returns_401(self, client: TestClient) -> None:
        """Benchmark status polling must require authentication.

        Test cases:
        - A request without a bearer session receives 401.
        """
        response = client.get("/benchmarks/status?ids=00000000-0000-0000-0000-000000000001")

        assert response.status_code == 401
