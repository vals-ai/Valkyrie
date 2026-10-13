"""Run with `uv run pytest tests/unit/api/test_run_settings.py`.

Cover the routes where a user keeps their own default sandbox provider and benchmark services.
"""

from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import Request
from fastapi.testclient import TestClient
from sqlmodel import Session

import tracker.api.run_settings as run_settings_api
import tracker.api.secrets as secrets_api
from main import app
from tests.utils import TEST_ORG_ID
from tracker.auth import RequestIdentity, get_current_identity
from tracker.database.models import Org, UserBenchmarkService, UserDefaultProvider
from tracker.exceptions import SecretNotFoundError

_OTHER_ORG_ID = UUID("00000000-0000-0000-0000-000000000002")
_NOTHING_SAVED: dict[str, object] = {"default_provider": None, "benchmark_services": {}}


def _service(url: str, header: str | None = None, secret: str | None = None) -> dict[str, str | None]:
    return {"url": url, "auth_header_name": header, "auth_secret_name": secret}


@pytest.fixture
def client_as(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], TestClient]:
    org = Org(id=TEST_ORG_ID, name="default", storage_key="test-bucket")

    def identity(request: Request) -> RequestIdentity:
        headers = request.headers
        return RequestIdentity(org=org, user_id=headers.get("x-test-user"), access_key_id=headers.get("x-test-key"))

    monkeypatch.setitem(app.dependency_overrides, get_current_identity, identity)
    monkeypatch.setattr(run_settings_api, "AUTH_REQUIRED", True)
    return lambda user_id: TestClient(app, headers={"x-test-user": user_id})


def test_users_keep_their_own_default_and_urls(client_as: Callable[[str], TestClient]) -> None:
    alice, bob = client_as("U-alice"), client_as("U-bob")

    alice.put("/settings/default-provider", json={"provider": "daytona"})
    replaced = alice.put("/settings/default-provider", json={"provider": "modal"})
    alice.put("/settings/benchmark-services/swebench", json={"url": "https://swe.example.com/"})

    assert replaced.json() == {"default_provider": "modal", "benchmark_services": {}}
    assert bob.delete("/settings/benchmark-services/swebench").status_code == 404
    assert alice.get("/settings").json() == {
        "default_provider": "modal",
        "benchmark_services": {"swebench": _service("https://swe.example.com")},
    }
    assert bob.get("/settings").json() == _NOTHING_SAVED


def test_a_user_id_in_another_organization_is_a_different_user(
    client_as: Callable[[str], TestClient], database_session: Session
) -> None:
    database_session.add(Org(id=_OTHER_ORG_ID, name="other", storage_key="other-bucket"))
    database_session.add(UserDefaultProvider(org_id=_OTHER_ORG_ID, user_id="U", provider="modal"))
    database_session.add(UserBenchmarkService(org_id=_OTHER_ORG_ID, user_id="U", benchmark="x", url="https://x.dev"))
    database_session.commit()
    user = client_as("U")

    assert user.get("/settings").json() == _NOTHING_SAVED
    assert user.delete("/settings/benchmark-services/x").status_code == 404


def test_removing_a_saved_url(client_as: Callable[[str], TestClient]) -> None:
    user = client_as("U")
    user.put("/settings/benchmark-services/swebench", json={"url": "https://swe.example.com"})
    user.put("/settings/benchmark-services/other", json={"url": "https://other.example.com"})

    removed = user.delete("/settings/benchmark-services/swebench")
    removed_again = user.delete("/settings/benchmark-services/swebench")

    assert removed.json() == {
        "default_provider": None,
        "benchmark_services": {"other": _service("https://other.example.com")},
    }
    assert removed_again.status_code == 404
    assert user.get("/settings").json() == removed.json()


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("put", "/settings/default-provider", {"provider": "modal"}),
        ("put", "/settings/benchmark-services/swebench", {"url": "https://swe.example.com"}),
        ("delete", "/settings/benchmark-services/swebench", None),
    ],
)
@pytest.mark.parametrize("key_user", [None, "U"], ids=["key-without-a-user", "key-of-a-user"])
def test_keys_read_their_users_settings_but_cannot_change_them(
    client_as: Callable[[str], TestClient],
    method: str,
    path: str,
    body: dict[str, str] | None,
    key_user: str | None,
) -> None:
    user = client_as("U")
    saved = user.put("/settings/benchmark-services/swebench", json={"url": "https://swe.example.com"}).json()
    keyed = TestClient(app, headers={"x-test-key": "K", **({"x-test-user": key_user} if key_user else {})})

    response = keyed.request(method, path, json=body)

    assert response.status_code == 403
    assert response.json()["detail"] == (
        "Only a signed-in user can change run settings; sign in with `valkyrie login` and unset VALKYRIE_API_KEY"
    )
    assert keyed.get("/settings").json() == (saved if key_user else _NOTHING_SAVED)
    assert user.get("/settings").json() == saved


def test_saved_urls_must_be_allowed_destinations(client_as: Callable[[str], TestClient]) -> None:
    user = client_as("U")

    refused = user.put("/settings/benchmark-services/x", json={"url": "http://10.0.0.1:8001"})
    allowed = user.put("/settings/benchmark-services/x", json={"url": "https://swe.example.com"})

    assert refused.status_code == 403
    assert refused.json() == {"detail": "Custom benchmark destination is not allowed"}
    assert allowed.json()["benchmark_services"] == {"x": _service("https://swe.example.com")}


def test_a_saved_service_names_a_secret_the_organization_has(
    client_as: Callable[[str], TestClient], monkeypatch: pytest.MonkeyPatch
) -> None:
    store = MagicMock(get=AsyncMock(side_effect=[{}, {}, SecretNotFoundError("missing")]))
    monkeypatch.setattr(secrets_api, "secret_store", MagicMock(return_value=store))
    monkeypatch.setattr(secrets_api, "AUTH_REQUIRED", True)
    user, url = client_as("U"), "https://swe.example.com"

    default = user.put("/settings/benchmark-services/a", json={"url": url, "auth_secret_name": "key"})
    named = user.put(
        "/settings/benchmark-services/b",
        json={"url": url, "auth_secret_name": "key", "auth_header_name": "X-Descope-Api-Key"},
    )
    missing = user.put("/settings/benchmark-services/c", json={"url": url, "auth_secret_name": "nope"})

    assert default.json()["benchmark_services"]["a"] == _service(url, "Authorization", "key")
    assert named.json()["benchmark_services"]["b"] == _service(url, "X-Descope-Api-Key", "key")
    assert missing.status_code == 404
    assert "c" not in user.get("/settings").json()["benchmark_services"]


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/settings/default-provider", {"provider": "Modal"}),
        ("/settings/default-provider", {}),
        ("/settings/benchmark-services/Bad_Name", {"url": "https://swe.example.com"}),
        ("/settings/benchmark-services/swebench", {"url": "ftp://swe.example.com"}),
        ("/settings/benchmark-services/swebench", {}),
        ("/settings/benchmark-services/swebench", {"url": "https://swe.example.com", "auth_secret_name": "bad name"}),
        ("/settings/benchmark-services/swebench", {"url": "https://swe.example.com", "auth_header_name": "X-Key"}),
        (
            "/settings/benchmark-services/swebench",
            {"url": "https://a.dev", "auth_secret_name": "k", "auth_header_name": "Host"},
        ),
    ],
)
def test_invalid_providers_benchmarks_urls_and_auth_are_rejected(
    client_as: Callable[[str], TestClient], path: str, body: dict[str, Any]
) -> None:
    user = client_as("U")

    assert user.put(path, json=body).status_code == 422
    assert user.get("/settings").json() == _NOTHING_SAVED
