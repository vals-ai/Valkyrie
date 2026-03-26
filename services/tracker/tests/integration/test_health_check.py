import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine

from main import app

client = TestClient(app)


class TestHealthCheckIntegration:
    def test_health_check_with_database(self, postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch):
        """
        Test health check with a real postgres database.

        Test Cases:
            - Returns 200 OK when database is accessible
            - Response contains expected format
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
        """
        Test health check returns 503 when database is unavailable.

        Test Cases:
            - Returns 503 Service Unavailable
            - Response contains error detail
        """
        # Mock check_database_connection to return False (simulating DB down)
        monkeypatch.setattr("main.check_database_connection", lambda: False)

        response = client.get("/health")

        assert response.status_code == 503
        assert response.json() == {"detail": "Database is not accessible"}
