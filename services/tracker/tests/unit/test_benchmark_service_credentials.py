"""Tests for Tracker-owned managed benchmark-service credentials."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock
from uuid import uuid4

from botocore.exceptions import ClientError
import pytest

import tracker.benchmark_service_credentials as credentials
from tracker.aws.clients import AWSClientProvider
from tracker.database.models import Org
from tracker.exceptions import TrackerServiceError


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "SecretsManager")


class _SecretsManager:
    def __init__(self, reads: list[str | ClientError], create_error: Exception | None = None) -> None:
        self.reads = reads
        self.create_error = create_error
        self.created: dict[str, str] | None = None

    def get_secret_value(self, *, SecretId: str) -> dict[str, str]:
        value = self.reads.pop(0)
        if isinstance(value, ClientError):
            raise value
        return {"SecretString": value}

    def create_secret(self, *, Name: str, SecretString: str) -> None:
        self.created = {"Name": Name, "SecretString": SecretString}
        if self.create_error:
            raise self.create_error


def _provider(secrets: _SecretsManager) -> AWSClientProvider:
    return cast(AWSClientProvider, SimpleNamespace(secretsmanager_client=lambda: secrets))


@pytest.fixture
def org() -> Org:
    return Org(id=uuid4(), name="tenant")


@pytest.fixture(autouse=True)
def managed_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        credentials.config,
        "BENCHMARK_SERVICE_ACCESS_KEY_SECRET_PREFIX",
        "/vals/test/benchmark-services",
    )
    monkeypatch.setattr(credentials.config, "DESCOPE_PROJECT_ID", "project")
    monkeypatch.setattr(credentials.config, "DESCOPE_MANAGEMENT_KEY", "management-key")


def _descope_client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    client = MagicMock()
    client.mgmt.access_key.create.return_value = {
        "key": {"id": "new-key-id"},
        "cleartext": "new-cleartext",
    }
    monkeypatch.setattr(credentials, "DescopeClient", MagicMock(return_value=client))
    return client


def test_loads_existing_credential_without_descope(
    org: Org,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descope = _descope_client(monkeypatch)
    secrets = _SecretsManager([json.dumps({"id": "key-id", "cleartext": "cleartext"})])

    headers = credentials.managed_benchmark_service_headers(org, _provider(secrets))

    assert headers == {"X-Descope-Api-Key": "cleartext"}
    descope.mgmt.access_key.create.assert_not_called()


def test_creates_and_persists_org_credential(
    org: Org,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descope = _descope_client(monkeypatch)
    secrets = _SecretsManager([_client_error("ResourceNotFoundException")])

    headers = credentials.ensure_managed_benchmark_service_headers(org, _provider(secrets))

    assert headers == {"X-Descope-Api-Key": "new-cleartext"}
    create_call = descope.mgmt.access_key.create.call_args.kwargs
    assert create_call["key_tenants"][0].tenant_id == org.name
    assert create_call["custom_claims"] == {"purpose": "valkyrie_benchmark_service"}
    assert secrets.created == {
        "Name": f"/vals/test/benchmark-services/{org.id}",
        "SecretString": json.dumps({"id": "new-key-id", "cleartext": "new-cleartext"}),
    }


def test_concurrent_creator_revokes_loser_and_uses_winner(
    org: Org,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descope = _descope_client(monkeypatch)
    secrets = _SecretsManager(
        [
            _client_error("ResourceNotFoundException"),
            json.dumps({"id": "winner-id", "cleartext": "winner-cleartext"}),
        ],
        create_error=_client_error("ResourceExistsException"),
    )

    headers = credentials.ensure_managed_benchmark_service_headers(org, _provider(secrets))

    assert headers == {"X-Descope-Api-Key": "winner-cleartext"}
    descope.mgmt.access_key.deactivate.assert_called_once_with("new-key-id")


def test_persistence_failure_revokes_created_key(
    org: Org,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descope = _descope_client(monkeypatch)
    secrets = _SecretsManager(
        [_client_error("ResourceNotFoundException")],
        create_error=_client_error("AccessDeniedException"),
    )

    with pytest.raises(TrackerServiceError, match="Failed to store"):
        credentials.ensure_managed_benchmark_service_headers(org, _provider(secrets))

    descope.mgmt.access_key.deactivate.assert_called_once_with("new-key-id")


def test_timeout_after_commit_keeps_stored_key_active(
    org: Org,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descope = _descope_client(monkeypatch)
    stored = json.dumps({"id": "new-key-id", "cleartext": "new-cleartext"})
    secrets = _SecretsManager(
        [_client_error("ResourceNotFoundException"), stored],
        create_error=TimeoutError("response lost after commit"),
    )

    headers = credentials.ensure_managed_benchmark_service_headers(org, _provider(secrets))

    assert headers == {"X-Descope-Api-Key": "new-cleartext"}
    descope.mgmt.access_key.deactivate.assert_not_called()


def test_missing_worker_credential_does_not_create_one(
    org: Org,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descope = _descope_client(monkeypatch)
    secrets = _SecretsManager([_client_error("ResourceNotFoundException")])

    with pytest.raises(TrackerServiceError, match="is not configured"):
        credentials.managed_benchmark_service_headers(org, _provider(secrets))

    descope.mgmt.access_key.create.assert_not_called()
