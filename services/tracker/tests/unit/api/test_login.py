from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock

import pytest
from descope.exceptions import AuthException
from fastapi.testclient import TestClient

from main import app
from tests.utils import TEST_ORG_ID, FakeAccessKeys
from tracker.auth import RequestIdentity, get_current_starter
from tracker.database.models import Org

_client = TestClient(app, headers={"Authorization": "Bearer session"})
_ORG = Org(id=TEST_ORG_ID, name="acme", storage_key="test-bucket")


class _LoadableAccessKeys(FakeAccessKeys):
    def __init__(self) -> None:
        super().__init__()
        self.stored: dict[str, dict[str, Any]] = {}
        self.missing_status = 404

    def create(self, name: str, expire_time: int = 0, **kwargs: Any) -> dict[str, Any]:
        response = super().create(name, expire_time, **kwargs)
        key_id = response["key"]["id"]
        tenants = [{"tenantId": tenant.tenant_id, "roleNames": []} for tenant in kwargs["key_tenants"]]
        self.stored[key_id] = {"id": key_id, "keyTenants": tenants, "customClaims": kwargs["custom_claims"]}
        return response

    def load(self, id: str) -> dict[str, Any]:
        if id not in self.stored:
            raise AuthException(self.missing_status, "E", "not found")
        return {"key": self.stored[id]}


@pytest.fixture
def access_keys(monkeypatch: pytest.MonkeyPatch) -> _LoadableAccessKeys:
    fake = _LoadableAccessKeys()
    user = {"email": "alice@example.com", "verifiedEmail": True, "userTenants": [{"tenantId": "acme"}]}
    management = MagicMock(access_key=fake)
    management.user.load_by_user_id.return_value = {"user": user}
    monkeypatch.setattr("tracker.auth.descope_management", lambda: management)
    monkeypatch.setattr("tracker.auth._validate_bearer", MagicMock(return_value={"sub": "U-alice"}))
    return fake


def test_login_settings_need_a_cli_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tracker.api.login.AUTH_REQUIRED", True)
    monkeypatch.setattr("tracker.api.login.DESCOPE_PROJECT_ID", "P123")
    monkeypatch.setattr("tracker.api.login.DESCOPE_CLI_CLIENT_ID", "")
    assert _client.get("/auth/cli").status_code == 404

    monkeypatch.setattr("tracker.api.login.DESCOPE_CLI_CLIENT_ID", "client-1")
    response = _client.get("/auth/cli")

    assert response.status_code == 200
    assert response.json() == {
        "device_url": "https://api.descope.com/oauth2/v1/P123/device",
        "token_url": "https://api.descope.com/oauth2/v1/P123/token",
        "client_id": "client-1",
        "project_id": "P123",
        "refresh_url": "https://api.descope.com/v1/auth/refresh",
    }


@pytest.mark.parametrize(
    ("identity", "credential"),
    [({}, "login"), ({"access_key_id": "K1"}, "access_key")],
)
def test_whoami_reports_the_credential(
    monkeypatch: pytest.MonkeyPatch, identity: dict[str, Any], credential: str
) -> None:
    starter = RequestIdentity(org=_ORG, email="alice@example.com", name="Alice", user_id="U-alice", **identity)
    monkeypatch.setitem(app.dependency_overrides, get_current_starter, lambda: starter)

    response = _client.get("/auth/me")

    assert response.status_code == 200
    assert response.json() == {
        "organization": "acme",
        "email": "alice@example.com",
        "name": "Alice",
        "credential": credential,
    }


def test_logged_in_users_create_org_scoped_expiring_keys(access_keys: _LoadableAccessKeys) -> None:
    response = _client.post("/access-keys", json={"name": "ci", "expires_in_days": 7})

    assert response.status_code == 200, response.text
    assert response.json()["key"] == "run-secret-K1"
    [created] = access_keys.created
    assert [tenant.tenant_id for tenant in created["key_tenants"]] == ["acme"]
    assert "user_id" not in created
    assert created["custom_claims"] == {"user_id": "U-alice", "valkyrie_user_key": True, "email": "alice@example.com"}
    assert created["expire_time"] == pytest.approx(time.time() + 7 * 86400, abs=60)


@pytest.mark.parametrize("descope_status", [404, 503])
def test_users_revoke_only_their_own_keys(access_keys: _LoadableAccessKeys, descope_status: int) -> None:
    access_keys.missing_status = descope_status
    _client.post("/access-keys", json={"name": "mine"})
    access_keys.stored["K-other"] = {
        "id": "K-other",
        "keyTenants": [{"tenantId": "acme"}],
        "customClaims": {"user_id": "U-bob"},
    }
    access_keys.stored["K-elsewhere"] = {
        "id": "K-elsewhere",
        "keyTenants": [{"tenantId": "other-org"}],
        "customClaims": {"user_id": "U-alice"},
    }

    assert _client.delete("/access-keys/K-other").status_code == 404
    assert _client.delete("/access-keys/K-elsewhere").status_code == 404
    assert _client.delete("/access-keys/K-missing").status_code == descope_status
    assert _client.delete("/access-keys/K1").status_code == 204
    assert access_keys.deleted == ["K1"]
