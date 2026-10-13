"""Run with `uv run pytest tests/unit/api/test_secrets.py`.

Cover the routes an organization uses to store its own run secrets.
"""

import json
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import tracker.api.secrets as secrets_api
from tracker import config
from tracker.aws.secrets import SecretBeingDeletedError, SecretsManagerStore
from main import app
from tests.utils import TEST_ORG_ID
from tracker.auth import LOGIN_REQUIRED, LoginIdentity, get_current_org
from tracker.database.models import Org
from tracker.local import config as local_config
from tracker.local.resources import LocalResources

_client = TestClient(app)
_PREFIX = f"valkyrie/tenants/{TEST_ORG_ID}/"


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    secret_store = MagicMock(spec=SecretsManagerStore)
    secret_store.list_names.return_value = []
    secret_store.delete.return_value = True
    monkeypatch.setattr(secrets_api, "resolve_org_aws_runtime", lambda _org: MagicMock())
    monkeypatch.setattr(secrets_api, "SecretsManagerStore", lambda _clients: secret_store)
    return secret_store


def test_set_stores_the_secret_under_the_org_prefix_with_its_tags(store: MagicMock) -> None:
    response = _client.put("/secrets/daytona", json={"value": '{"api_key": "x"}'})

    assert response.status_code == 200
    assert response.json() == {"name": "daytona", "reference": f"{_PREFIX}daytona"}
    store.put.assert_awaited_once_with(
        f"{_PREFIX}daytona", '{"api_key": "x"}', tags={"valkyrie:org-id": str(TEST_ORG_ID)}
    )


@pytest.mark.parametrize("name", ["bad name", "a$b", "x" * 129])
def test_set_rejects_invalid_names(store: MagicMock, name: str) -> None:
    response = _client.put(f"/secrets/{name}", json={"value": "value"})

    assert response.status_code == 422
    store.put.assert_not_awaited()


@pytest.mark.parametrize(
    "value",
    ["", "s3cret-" * 10000, "s3cret-\ud800", "s3cret-" + "é" * 40000],
    ids=["empty", "long", "surrogate", "bytes"],
)
def test_set_rejects_bad_values_without_echoing_them(store: MagicMock, value: str) -> None:
    response = _client.put(
        "/secrets/daytona", content=json.dumps({"value": value}), headers={"Content-Type": "application/json"}
    )

    assert response.status_code == 400
    assert "s3cret" not in response.text
    store.put.assert_not_awaited()


@pytest.mark.parametrize(
    "body",
    [{"value": {"token": "s3cret"}}, {"value": ["s3cret"]}, {"token": "s3cret"}, "s3cret"],
    ids=["object", "list", "missing", "string"],
)
def test_set_rejects_malformed_bodies_without_echoing_them(store: MagicMock, body: object) -> None:
    response = _client.put("/secrets/daytona", json=body)

    assert response.status_code == 422
    assert "s3cret" not in response.text
    store.put.assert_not_awaited()


def test_list_returns_names_and_references_without_values(store: MagicMock) -> None:
    store.list_names.return_value = [f"{_PREFIX}daytona", f"{_PREFIX}modal"]

    response = _client.get("/secrets")

    assert response.status_code == 200
    assert response.json() == {
        "secrets": [
            {"name": "daytona", "reference": f"{_PREFIX}daytona"},
            {"name": "modal", "reference": f"{_PREFIX}modal"},
        ]
    }
    store.list_names.assert_awaited_once_with(_PREFIX)


@pytest.mark.parametrize(("deleted", "status"), [(True, 200), (False, 404)])
def test_remove_reports_a_missing_secret_as_not_existing(store: MagicMock, deleted: bool, status: int) -> None:
    store.delete.return_value = deleted

    response = _client.delete("/secrets/daytona")

    assert response.status_code == status
    store.delete.assert_awaited_once_with(f"{_PREFIX}daytona")
    if not deleted:
        assert response.json()["detail"] == "Secret 'daytona' does not exist"


def test_local_runs_have_no_secret_storage(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(local_config, "resources", LocalResources(data_root=tmp_path))

    response = _client.get("/secrets")

    assert response.status_code == 400


@pytest.mark.parametrize(
    ("url", "shared_tenants", "status"),
    [
        ("http://169.254.170.2/v2/credentials", "", 400),
        ("http://10.0.0.5/api", "", 400),
        ("https://daytona.internal/api", "", 400),
        ("https://app.daytona.io/api", "", 200),
        ("http://10.0.0.5/api", "default", 200),
    ],
    ids=["ecs-credentials", "private", "internal-name", "public", "trusted-tenant"],
)
def test_set_keeps_provider_endpoints_off_internal_hosts(
    store: MagicMock, monkeypatch: pytest.MonkeyPatch, url: str, shared_tenants: str, status: int
) -> None:
    monkeypatch.setattr(secrets_api, "AUTH_REQUIRED", True)
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", shared_tenants)
    value = json.dumps({"DAYTONA_API_KEY": "k", "DAYTONA_API_URL": url, "DAYTONA_TARGET": "us"})

    response = _client.put("/secrets/daytona", json={"value": value})

    assert response.status_code == status
    assert store.put.await_count == (status == 200)


def test_set_reports_a_name_still_being_deleted(store: MagicMock) -> None:
    store.put.side_effect = SecretBeingDeletedError("still being deleted")

    response = _client.put("/secrets/daytona", json={"value": "value"})

    assert response.status_code == 409


@pytest.mark.parametrize(
    ("shared_tenants", "reference"),
    [("internal.example", "daytona"), ("default", f"{_PREFIX}daytona")],
    ids=["customer", "shared-tenant"],
)
def test_hosted_set_and_list_report_the_name_runs_use(
    store: MagicMock, monkeypatch: pytest.MonkeyPatch, shared_tenants: str, reference: str
) -> None:
    monkeypatch.setattr(secrets_api, "AUTH_REQUIRED", True)
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", shared_tenants)
    store.list_names.return_value = [f"{_PREFIX}daytona"]

    saved = _client.put("/secrets/daytona", json={"value": "v"})
    listed = _client.get("/secrets")

    assert saved.json() == {"name": "daytona", "reference": reference}
    assert listed.json() == {"secrets": [{"name": "daytona", "reference": reference}]}


@pytest.mark.parametrize(
    ("roles", "detail"),
    [
        ({"vals.ai": ("Valkyrie Admin",)}, None),
        ({"vals.ai": ()}, "Only Valkyrie Admins of vals.ai can change its secrets"),
        ({"acme": ("Valkyrie Admin",), "vals.ai": ()}, "Only Valkyrie Admins of vals.ai can change its secrets"),
        (None, LOGIN_REQUIRED),
    ],
    ids=["admin-login", "member-login", "admin-of-another-organization", "access-key"],
)
def test_on_bench_only_admins_change_vals_ai_secrets(
    store: MagicMock, monkeypatch: pytest.MonkeyPatch, roles: dict[str, tuple[str, ...]] | None, detail: str | None
) -> None:
    monkeypatch.setattr(secrets_api, "VALKYRIE_STAGE", "bench")
    vals = Org(id=TEST_ORG_ID, name="vals.ai", storage_key="test-bucket")
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: vals)
    login = HTTPException(403, LOGIN_REQUIRED) if roles is None else LoginIdentity("U-1", "alice", None, roles, None)
    monkeypatch.setattr(secrets_api, "get_current_login", MagicMock(side_effect=[login]))

    response = _client.put("/secrets/daytona", json={"value": "v"})

    assert response.status_code == (200 if detail is None else 403), response.text
    assert store.put.await_count == (detail is None)
    assert detail is None or detail in response.json()["detail"]


@pytest.mark.usefixtures("store")
@pytest.mark.parametrize(("stage", "org"), [("bench", "default"), ("dev", "vals.ai"), ("", "vals.ai")])
def test_other_organizations_and_stages_need_no_admin(monkeypatch: pytest.MonkeyPatch, stage: str, org: str) -> None:
    monkeypatch.setattr(secrets_api, "VALKYRIE_STAGE", stage)
    acting = Org(id=TEST_ORG_ID, name=org, storage_key="test-bucket")
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: acting)

    assert _client.put("/secrets/daytona", json={"value": "v"}).status_code == 200
    assert _client.delete("/secrets/daytona").status_code == 200
