"""Run with `uv run pytest tests/integration/local/api/test_agents.py`.

Exercise authentication for agent routes through the real app.
"""

from fastapi.testclient import TestClient


def test_agents_unauth_401(client: TestClient) -> None:
    """The agents catalog must not be readable without authentication.

    Test cases:
    - A request without a bearer session receives 401.
    """
    response = client.get("/agents")

    assert response.status_code == 401
