"""Run with `uv run pytest tests/integration/local/api/test_filter_options.py`.

Exercise benchmark filter options through the real app and local database.
"""

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.utils import TEST_ORG_ID
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
)


def _persist_benchmark(session: Session, name: str, agent: str) -> Benchmark:
    """Persist one benchmark contributing filter metadata."""
    benchmark = Benchmark(
        org_id=TEST_ORG_ID,
        name=name,
        status=BenchmarkStatus.FINISHED,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name=agent, install_cmd="i", run_cmd="r"),
            concurrency=1,
        ),
    )
    session.add(benchmark)
    session.commit()

    return benchmark


class TestFilterOptions:
    """Filter option responses and authentication."""

    def test_filter_options_returns_distinct(self, client: TestClient, database_session: Session) -> None:
        """Filter options must collapse repeated benchmark metadata into distinct values.

        Test cases:
        - Authenticated results contain each available filter value once.
        """
        for benchmark_name, agent_name in [
            ("swebench", "mini_sweagent"),
            ("swebench", "claude_code"),
            ("fab", "mini_sweagent"),
            ("swebench", "mini_sweagent"),
        ]:
            _persist_benchmark(database_session, benchmark_name, agent_name)

        response = client.get(
            "/benchmarks/filter-options",
            headers={"Authorization": "Bearer fake"},
        )

        assert response.status_code == 200, response.text
        response_body = response.json()
        assert response_body["benchmark_names"] == ["fab", "swebench"]
        assert sorted(response_body["agent_names"]) == ["claude_code", "mini_sweagent"]

    def test_filter_options_unauth_401(self, client: TestClient) -> None:
        """Benchmark filter metadata must require authentication.

        Test cases:
        - A request without a bearer session receives 401.
        """
        response = client.get("/benchmarks/filter-options")

        assert response.status_code == 401
