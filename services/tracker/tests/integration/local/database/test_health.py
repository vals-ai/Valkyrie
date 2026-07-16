"""Run with `uv run pytest tests/integration/local/database/test_health.py`.

Exercise tracker health checks against disposable Postgres.
"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine

from main import app

_client = TestClient(app)


class TestHealthCheckIntegration:
    """Health endpoint behavior against available and unavailable databases."""

    def test_health_check_with_database(
        self,
        postgres_engine: Engine,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify the health endpoint succeeds when the integration database is reachable.

        Test cases:
        - The endpoint returns 200 when check_database_connection uses the test Postgres engine.
        - The response body matches the public health-check success payload.
        """
        import tracker.database.session as session_module

        monkeypatch.setattr(session_module, "engine", postgres_engine)

        response = _client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_health_check_database_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify the health endpoint reports database connectivity failures.

        Test cases:
        - The endpoint returns 503 when check_database_connection reports failure.
        - The response body includes the public database-unavailable detail.
        """
        monkeypatch.setattr("main.check_database_connection", lambda: False)

        response = _client.get("/health")

        assert response.status_code == 503
        assert response.json() == {"detail": "Database is not accessible"}
