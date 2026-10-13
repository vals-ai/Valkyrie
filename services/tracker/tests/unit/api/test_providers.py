"""Run with `uv run pytest tests/unit/api/test_providers.py`.

Cover the routes an organization uses to map sandbox providers to its own secrets.
"""

from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

import tracker.api.secrets as secrets_api
from main import app
from tests.utils import TEST_ORG_ID
from tracker import config
from tracker.auth import LoginIdentity, get_current_org
from tracker.aws.secrets import SecretsManagerStore
from tracker.database.models import Org, OrgProviderSecret
from tracker.exceptions import SecretNotFoundError

_client = TestClient(app)
_PREFIX = f"valkyrie/tenants/{TEST_ORG_ID}/"
_OTHER_ORG_ID = UUID("00000000-0000-0000-0000-000000000002")


@pytest.fixture
def store(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    secret_store = MagicMock(spec=SecretsManagerStore)
    _stored(secret_store)
    monkeypatch.setattr(secrets_api, "secret_store", MagicMock(return_value=secret_store))
    monkeypatch.setattr(secrets_api, "AUTH_REQUIRED", True)
    return secret_store


def _stored(store: MagicMock, *names: str) -> None:
    """Let the organization's store hold these secrets, as AWS answers a read of one by its full name."""

    def get(reference: str) -> dict[str, str]:
        if reference not in {f"{_PREFIX}{name}" for name in names}:
            raise SecretNotFoundError(f"Secret '{reference}' does not exist")
        return {}

    store.get.side_effect = get


def test_members_map_a_provider_to_an_existing_secret(store: MagicMock) -> None:
    _stored(store, "thissecret", "thatsecret")

    mapped = _client.put("/sandbox-providers/daytona", json={"secret_name": "thissecret"})
    replaced = _client.put("/sandbox-providers/daytona", json={"secret_name": "thatsecret"})
    listed = _client.get("/sandbox-providers")
    removed = _client.delete("/sandbox-providers/daytona")
    removed_again = _client.delete("/sandbox-providers/daytona")

    assert mapped.json() == {"providers": {"daytona": "thissecret"}}
    assert replaced.json() == {"providers": {"daytona": "thatsecret"}}
    assert listed.json() == {"providers": {"daytona": "thatsecret"}}
    assert removed.json() == {"providers": {}}
    assert removed_again.status_code == 404
    assert removed_again.json()["detail"] == "Provider 'daytona' has no secret"
    store.get.assert_awaited_with(f"{_PREFIX}thatsecret")


@pytest.mark.parametrize(
    ("secret_name", "looked_up"),
    [
        ("nope", [f"{_PREFIX}nope"]),
        (f"valkyrie/tenants/{_OTHER_ORG_ID}/other", []),
        ("valkyrie/daytona", [f"{_PREFIX}valkyrie/daytona"]),
    ],
    ids=["missing", "other-organization", "shared"],
)
def test_mapping_a_secret_outside_the_organizations_store_is_refused(
    store: MagicMock, secret_name: str, looked_up: list[str]
) -> None:
    response = _client.put("/sandbox-providers/daytona", json={"secret_name": secret_name})

    assert response.status_code == 404
    assert response.json()["detail"] == (
        f"Secret '{secret_name}' does not exist; create it with `valkyrie secret set {secret_name}`"
    )
    assert [awaited.args[0] for awaited in store.get.await_args_list] == looked_up
    assert _client.get("/sandbox-providers").json() == {"providers": {}}


def test_shared_secret_tenants_map_shared_names_without_a_store_check(
    store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "default")

    response = _client.put("/sandbox-providers/daytona", json={"secret_name": "valkyrie/daytona"})

    assert response.json() == {"providers": {"daytona": "valkyrie/daytona"}}
    store.get.assert_not_awaited()


def test_an_organization_only_sees_its_own_mappings(store: MagicMock, database_session: Session) -> None:
    database_session.add(Org(id=_OTHER_ORG_ID, name="other", storage_key="other-bucket"))
    database_session.add(OrgProviderSecret(org_id=_OTHER_ORG_ID, provider="modal", secret_name="theirs"))
    database_session.commit()
    _stored(store, "mine")

    _client.put("/sandbox-providers/daytona", json={"secret_name": "mine"})

    assert _client.get("/sandbox-providers").json() == {"providers": {"daytona": "mine"}}
    assert _client.delete("/sandbox-providers/modal").status_code == 404


@pytest.mark.parametrize(
    ("provider", "body"),
    [
        ("Daytona", {"secret_name": "mine"}),
        ("-daytona", {"secret_name": "mine"}),
        ("x" * 65, {"secret_name": "mine"}),
        ("daytona", {"secret_name": ""}),
        ("daytona", {"secret_name": "bad name"}),
        ("daytona", {"secret_name": "x" * 513}),
        ("daytona", {}),
    ],
)
def test_invalid_providers_and_secret_names_are_rejected(provider: str, body: dict[str, str]) -> None:
    assert _client.put(f"/sandbox-providers/{provider}", json=body).status_code == 422
    assert _client.get("/sandbox-providers").json() == {"providers": {}}


@pytest.mark.parametrize(
    ("roles", "status"),
    [
        ((), 403),
        (("Valkyrie Admin",), 200),
    ],
    ids=["member", "admin"],
)
def test_only_admins_change_vals_ai_providers_on_bench(
    monkeypatch: pytest.MonkeyPatch, roles: tuple[str, ...], status: int
) -> None:
    monkeypatch.setattr(secrets_api, "VALKYRIE_STAGE", "bench")
    acting = Org(id=TEST_ORG_ID, name="vals.ai", storage_key="test-bucket")
    monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: acting)
    login = LoginIdentity(
        user_id="U", login_id="U", email=None, organizations={"vals.ai": roles}, organization="vals.ai"
    )
    monkeypatch.setattr(secrets_api, "get_current_login", MagicMock(return_value=login))

    put = _client.put("/sandbox-providers/daytona", json={"secret_name": "mine"})
    deleted = _client.delete("/sandbox-providers/daytona")

    assert (put.status_code, deleted.status_code) == (status, status), (put.text, deleted.text)
    assert _client.get("/sandbox-providers").json() == {"providers": {}}
