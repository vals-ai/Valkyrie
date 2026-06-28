import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine

from main import app

client = TestClient(app)


class TestHealthCheckIntegration:
    def test_health_check_with_database(self, postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch):
        """Verify the health endpoint succeeds when the integration database is reachable.

        Test cases:
        - The endpoint returns 200 when check_database_connection uses the test Postgres engine.
        - The response body matches the public health-check success payload.
        """
        import tracker.database.session as session_module

        # Override the engine used by check_database_connection
        monkeypatch.setattr(session_module, "engine", postgres_engine)

        response = client.get("/health")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

        # Clean up
        app.dependency_overrides.clear()

    def test_health_check_database_unavailable(self, monkeypatch: pytest.MonkeyPatch):
        """Verify the health endpoint reports database connectivity failures.

        Test cases:
        - The endpoint returns 503 when check_database_connection reports failure.
        - The response body includes the public database-unavailable detail.
        """
        # Mock check_database_connection to return False (simulating DB down)
        monkeypatch.setattr("main.check_database_connection", lambda: False)

        response = client.get("/health")

        assert response.status_code == 503
        assert response.json() == {"detail": "Database is not accessible"}
