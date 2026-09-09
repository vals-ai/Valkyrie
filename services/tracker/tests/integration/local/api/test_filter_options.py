"""Run with `uv run pytest tests/integration/local/api/test_filter_options.py`.

Exercise benchmark filter options through the real app and local database.
"""

from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark
from tracker.database.models import BenchmarkStatus


class TestFilterOptions:
    """Filter option responses and authentication."""

    def test_filter_options_returns_distinct(self, client: TestClient, database_session: Session) -> None:
        """Filter options must collapse repeated benchmark metadata into distinct values.

        Test cases:
        - Authenticated results contain each available filter value once.
        - Runs without a dataset are listed under "default", matching the run list and its dataset filter.
        """
        for benchmark_name, agent_name, model, dataset, email in [
            ("swebench", "mini_sweagent", "gateway/openai/gpt-5", "verified", "a@vals.ai"),
            ("swebench", "claude_code", "gateway/anthropic/claude-opus-5", None, "b@vals.ai"),
            ("fab", "mini_sweagent", None, "verified", None),
            ("swebench", "mini_sweagent", "gateway/openai/gpt-5", "lite", "a@vals.ai"),
        ]:
            make_benchmark(
                name=benchmark_name,
                status=BenchmarkStatus.FINISHED,
                agent_name=agent_name,
                model=model,
                dataset=dataset,
                started_by_email=email,
                session=database_session,
            )

        response = client.get(
            "/benchmarks/filter-options",
            headers={"Authorization": "Bearer fake"},
        )

        assert response.status_code == 200, response.text
        response_body = response.json()
        assert response_body["benchmark_names"] == ["fab", "swebench"]
        assert response_body["agent_names"] == ["claude_code", "mini_sweagent"]
        assert response_body["models"] == ["gateway/anthropic/claude-opus-5", "gateway/openai/gpt-5"]
        assert response_body["datasets"] == ["default", "lite", "verified"]
        assert response_body["started_by_emails"] == ["a@vals.ai", "b@vals.ai"]

    def test_filter_options_unauth_401(self, client: TestClient) -> None:
        """Benchmark filter metadata must require authentication.

        Test cases:
        - A request without a bearer session receives 401.
        """
        response = client.get("/benchmarks/filter-options")

        assert response.status_code == 401
