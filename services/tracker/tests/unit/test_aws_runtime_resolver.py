from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlmodel import Session
from starlette.requests import Request

from main import app
from tracker import config
from tracker.aws.clients import DefaultChainAWSClientProvider, ExplicitCredentialsAWSClientProvider
from tracker.aws.resolver import (
    resolve_aws_runtime_metadata,
    resolve_run_metadata_aws_runtime,
    resolve_run_aws_runtime,
)
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import Benchmark

_ORG_ID = UUID("00000000-0000-0000-0000-000000000001")
_OTHER_ORG_ID = UUID("00000000-0000-0000-0000-000000000002")

_COMPLETE_HARNESS_HEADERS = {
    "x-harness-aws-access-key-id": "header-access-key",
    "x-harness-aws-secret-access-key": "header-secret-key",
    "x-harness-aws-default-region": "header-region",
    "x-harness-aws-session-token": "header-session-token",
    "x-harness-s3-bucket": "header-bucket",
    "x-harness-log-group": "header-log-group",
    "x-harness-log-retention-policy": "14",
    "x-harness-sandbox-provider-secret-name": "header-provider-secret",
}


def _request(headers: dict[str, str] | None = None) -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "query_string": b"",
            "headers": [(key.encode(), value.encode()) for key, value in (headers or {}).items()],
        }
    )


def _configure_managed_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    eligible: bool = True,
    resources_configured: bool = True,
) -> None:
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", str(_ORG_ID if eligible else _OTHER_ORG_ID))
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_REGION", "deployment-region" if resources_configured else None)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_S3_BUCKET", "deployment-bucket" if resources_configured else None)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_GROUP", "deployment-log-group" if resources_configured else None)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "30" if resources_configured else None)


@pytest.mark.parametrize(
    ("aws_managed", "expected_bucket", "expected_provider"),
    [
        pytest.param(True, "deployment-bucket", DefaultChainAWSClientProvider, id="stored-managed"),
        pytest.param(False, "header-bucket", ExplicitCredentialsAWSClientProvider, id="stored-access-keys"),
    ],
)
def test_run_runtime_uses_stored_mode(
    monkeypatch: pytest.MonkeyPatch,
    aws_managed: bool,
    expected_bucket: str,
    expected_provider: type[DefaultChainAWSClientProvider] | type[ExplicitCredentialsAWSClientProvider],
) -> None:
    _configure_managed_runtime(monkeypatch)

    runtime = resolve_run_aws_runtime(
        _request(_COMPLETE_HARNESS_HEADERS),
        aws_managed=aws_managed,
        org_id=_ORG_ID,
    )

    assert runtime.resources.s3_bucket == expected_bucket
    assert isinstance(runtime.clients, expected_provider)


def test_managed_run_ignores_partial_access_key_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch)

    runtime = resolve_run_aws_runtime(
        _request({"x-harness-aws-access-key-id": "ignored"}),
        aws_managed=True,
        org_id=_ORG_ID,
    )

    assert runtime.resources.s3_bucket == "deployment-bucket"
    assert isinstance(runtime.clients, DefaultChainAWSClientProvider)


@pytest.mark.parametrize(
    ("aws_managed", "expected_bucket"),
    [
        pytest.param(True, "deployment-bucket", id="managed-metadata-has-runtime"),
        pytest.param(False, None, id="access-key-metadata-omits-aws-links"),
    ],
)
def test_optional_run_runtime_preserves_stored_mode(
    monkeypatch: pytest.MonkeyPatch,
    aws_managed: bool,
    expected_bucket: str | None,
) -> None:
    _configure_managed_runtime(monkeypatch)

    runtime = resolve_run_metadata_aws_runtime(
        _request(),
        aws_managed=aws_managed,
        org_id=_ORG_ID,
    )

    assert (runtime.resources.s3_bucket if runtime is not None else None) == expected_bucket


def test_run_runtime_rejects_managed_run_for_ineligible_org(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch, eligible=False)

    with pytest.raises(HTTPException) as exc_info:
        resolve_run_aws_runtime(
            _request(_COMPLETE_HARNESS_HEADERS),
            aws_managed=True,
            org_id=_ORG_ID,
        )

    assert exc_info.value.status_code == 403


@pytest.mark.parametrize("eligible", [True, False])
def test_deployment_runtime_metadata_requires_eligible_org(
    monkeypatch: pytest.MonkeyPatch,
    eligible: bool,
) -> None:
    _configure_managed_runtime(monkeypatch, eligible=eligible)

    resources = resolve_aws_runtime_metadata(_ORG_ID)

    assert (resources.s3_bucket if resources is not None else None) == ("deployment-bucket" if eligible else None)


def test_agent_list_uses_deployment_runtime_for_eligible_org(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    captured_runtime: AWSRuntime | None = None

    async def list_agents(runtime: AWSRuntime) -> list[object]:
        nonlocal captured_runtime
        captured_runtime = runtime
        return []

    monkeypatch.setattr("tracker.api.agents.list_agents", list_agents)
    response = TestClient(app).get("/agents")

    assert response.status_code == 200
    assert captured_runtime is not None
    assert captured_runtime.resources.s3_bucket == "deployment-bucket"


def test_managed_results_report_capped_presign_expiry(
    monkeypatch: pytest.MonkeyPatch,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    _configure_managed_runtime(monkeypatch)
    example_benchmark_object.aws_managed = True
    database_session.add(example_benchmark_object)
    database_session.commit()

    observed_expiration = MagicMock()

    async def _upload_final_view(*_args: Any, **_kwargs: Any) -> str:
        return "benchmarks/test/results.json"

    async def _create_presigned_url(*_args: Any, expiration: int, **_kwargs: Any) -> str:
        observed_expiration(expiration)
        return "https://example.test/results"

    monkeypatch.setattr("main.upload_final_view", _upload_final_view)
    monkeypatch.setattr("main.create_presigned_url", _create_presigned_url)

    response = TestClient(app).get(
        "/retrieve-results",
        params={"benchmark_id": str(example_benchmark_object.id), "s3": "true"},
    )

    assert response.status_code == 200
    assert response.json()["expires_in"] == 3600
    observed_expiration.assert_called_once_with(3600)
