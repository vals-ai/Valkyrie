"""Managed benchmark-service credentials owned by the Tracker deployment."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, cast

from botocore.exceptions import ClientError
from descope.descope_client import DescopeClient
from descope.management.common import AssociatedTenant

from tracker import config
from tracker.auth import BENCHMARK_SERVICE_API_KEY_HEADER, DESCOPE_BENCHMARK_SERVICE_PURPOSE
from tracker.aws.clients import AWSClientProvider
from tracker.database.models import Org
from tracker.exceptions import TrackerServiceError


@dataclass(frozen=True)
class _Credential:
    id: str
    cleartext: str = field(repr=False)


def _secret_name(org: Org) -> str:
    prefix = config.BENCHMARK_SERVICE_ACCESS_KEY_SECRET_PREFIX
    if not prefix:
        raise TrackerServiceError("BENCHMARK_SERVICE_ACCESS_KEY_SECRET_PREFIX is not configured")
    return f"{prefix.rstrip('/')}/{org.id}"


def _parse_credential(secret_string: str) -> _Credential:
    try:
        payload: object = json.loads(secret_string)
    except json.JSONDecodeError as exc:
        raise TrackerServiceError("Managed benchmark-service credential is invalid") from exc
    if not isinstance(payload, dict):
        raise TrackerServiceError("Managed benchmark-service credential is invalid")

    credential = cast(dict[str, object], payload)
    key_id = credential.get("id")
    cleartext = credential.get("cleartext")
    if not isinstance(key_id, str) or not isinstance(cleartext, str):
        raise TrackerServiceError("Managed benchmark-service credential is invalid")
    return _Credential(id=key_id, cleartext=cleartext)


def _load_credential(org: Org, clients: AWSClientProvider) -> _Credential | None:
    try:
        response: dict[str, Any] = clients.secretsmanager_client().get_secret_value(SecretId=_secret_name(org))
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return None
        raise TrackerServiceError("Failed to load managed benchmark-service credential") from exc

    return _parse_credential(str(response["SecretString"]))


def _create_descope_credential(org: Org) -> _Credential:
    if not config.DESCOPE_PROJECT_ID or not config.DESCOPE_MANAGEMENT_KEY:
        raise TrackerServiceError("Descope management is not configured")

    client = DescopeClient(
        project_id=config.DESCOPE_PROJECT_ID,
        management_key=config.DESCOPE_MANAGEMENT_KEY,
    )
    response = cast(
        dict[str, object],
        client.mgmt.access_key.create(  # pyright: ignore[reportUnknownMemberType]
            name=f"Valkyrie benchmark services ({org.name})",
            key_tenants=[AssociatedTenant(tenant_id=org.name)],
            custom_claims={"purpose": DESCOPE_BENCHMARK_SERVICE_PURPOSE},
        ),
    )
    key = response["key"]
    cleartext = response["cleartext"]
    assert isinstance(key, dict)
    key_id = cast(dict[str, object], key)["id"]
    assert isinstance(key_id, str)
    assert isinstance(cleartext, str)
    return _Credential(id=key_id, cleartext=cleartext)


def _deactivate_descope_credential(key_id: str) -> None:
    assert config.DESCOPE_PROJECT_ID
    assert config.DESCOPE_MANAGEMENT_KEY
    DescopeClient(
        project_id=config.DESCOPE_PROJECT_ID,
        management_key=config.DESCOPE_MANAGEMENT_KEY,
    ).mgmt.access_key.deactivate(key_id)


def managed_benchmark_service_headers(org: Org, clients: AWSClientProvider) -> dict[str, str]:
    """Load an existing org credential without creating one."""
    credential = _load_credential(org, clients)
    if credential is None:
        raise TrackerServiceError("Managed benchmark-service credential is not configured")
    return {BENCHMARK_SERVICE_API_KEY_HEADER: credential.cleartext}


def ensure_managed_benchmark_service_headers(org: Org, clients: AWSClientProvider) -> dict[str, str]:
    """Load or atomically create the org's benchmark-service credential."""
    credential = _load_credential(org, clients)
    if credential is not None:
        return {BENCHMARK_SERVICE_API_KEY_HEADER: credential.cleartext}

    credential = _create_descope_credential(org)
    secret_string = json.dumps({"id": credential.id, "cleartext": credential.cleartext})
    try:
        clients.secretsmanager_client().create_secret(
            Name=_secret_name(org),
            SecretString=secret_string,
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ResourceExistsException":
            winner = _load_credential(org, clients)
            assert winner is not None
            if winner.id != credential.id:
                _deactivate_descope_credential(credential.id)
            return {BENCHMARK_SERVICE_API_KEY_HEADER: winner.cleartext}
        _deactivate_descope_credential(credential.id)
        raise TrackerServiceError("Failed to store managed benchmark-service credential") from exc
    except Exception as exc:
        try:
            stored = _load_credential(org, clients)
        except TrackerServiceError:
            raise exc
        if stored is not None and stored.id == credential.id:
            return {BENCHMARK_SERVICE_API_KEY_HEADER: stored.cleartext}
        _deactivate_descope_credential(credential.id)
        raise

    return {BENCHMARK_SERVICE_API_KEY_HEADER: credential.cleartext}
