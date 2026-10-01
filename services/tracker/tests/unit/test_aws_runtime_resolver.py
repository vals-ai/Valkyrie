from collections.abc import Iterator
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlmodel import Session
from starlette.requests import Request

from main import app
from tracker import config
from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.aws.resolver import (
    resolve_managed_sandbox_provider,
    resolve_run_aws_runtime,
    resolve_run_metadata_aws_runtime,
    resolve_start_aws_runtime,
)
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import AgentContractRequest, Benchmark
from tracker.types import ManagedExecutionContext, RunExecutionRequest, StartBenchmarkRequest

_ORG_ID = UUID("00000000-0000-0000-0000-000000000001")
_OTHER_ORG_ID = UUID("00000000-0000-0000-0000-000000000002")

_HARNESS_HEADERS = {
    "x-harness-aws-access-key-id": "header-access-key",
    "x-harness-aws-secret-access-key": "header-secret-key",
    "x-harness-aws-default-region": "header-region",
    "x-harness-s3-bucket": "header-bucket",
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
    submissions_enabled: bool = True,
    resources_configured: bool = True,
) -> None:
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", str(_ORG_ID if eligible else _OTHER_ORG_ID))
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ACCOUNT_ID", "123456789012")
    monkeypatch.setattr(config, "AWS_MANAGED_SUBMISSIONS_ENABLED", submissions_enabled)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_REGION", "deployment-region" if resources_configured else None)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_S3_BUCKET", "deployment-bucket" if resources_configured else None)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_GROUP", "deployment-log-group" if resources_configured else None)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "30" if resources_configured else None)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_SANDBOX_PROVIDER", "daytona" if resources_configured else None)
    monkeypatch.setattr(
        config,
        "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME",
        "deployment-provider-secret" if resources_configured else None,
    )


@pytest.mark.parametrize(
    ("submissions_enabled", "eligible", "resources_configured", "expected_status"),
    [
        pytest.param(True, True, True, None, id="managed-eligible"),
        pytest.param(False, True, True, 503, id="managed-gate-closed"),
        pytest.param(True, False, True, 403, id="managed-ineligible"),
        pytest.param(True, True, False, 500, id="managed-config-missing"),
    ],
)
def test_start_runtime_selection(
    monkeypatch: pytest.MonkeyPatch,
    submissions_enabled: bool,
    eligible: bool,
    resources_configured: bool,
    expected_status: int | None,
) -> None:
    _configure_managed_runtime(
        monkeypatch,
        eligible=eligible,
        submissions_enabled=submissions_enabled,
        resources_configured=resources_configured,
    )

    if expected_status is not None:
        with pytest.raises(HTTPException) as exc_info:
            resolve_start_aws_runtime(_request(), _ORG_ID)
        assert exc_info.value.status_code == expected_status
        return

    runtime = resolve_start_aws_runtime(_request(), _ORG_ID)

    assert runtime.resources.s3_bucket == "deployment-bucket"
    assert isinstance(runtime.clients, DefaultChainAWSClientProvider)
    assert runtime.clients.credential_source == "managed"
    assert runtime.expected_bucket_owner == "123456789012"


@pytest.mark.parametrize(
    "resolver",
    [
        pytest.param(lambda request: resolve_start_aws_runtime(request, _ORG_ID), id="start"),
        pytest.param(lambda request: resolve_run_aws_runtime(request, aws_managed=True, org_id=_ORG_ID), id="run"),
        pytest.param(
            lambda request: resolve_run_metadata_aws_runtime(request, aws_managed=True, org_id=_ORG_ID),
            id="run-metadata",
        ),
    ],
)
def test_aws_request_headers_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    resolver: Any,
) -> None:
    """Requests carrying client-supplied AWS credentials fail instead of selecting a credentialed runtime."""
    _configure_managed_runtime(monkeypatch)

    with pytest.raises(HTTPException) as exc_info:
        resolver(_request(_HARNESS_HEADERS))

    assert exc_info.value.status_code == 400
    assert "AWS request headers are not accepted" in exc_info.value.detail


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param(_HARNESS_HEADERS, id="complete"),
        pytest.param({"x-harness-aws-access-key-id": "partial-access-key"}, id="partial"),
    ],
)
def test_run_runtime_rejects_any_harness_header(monkeypatch: pytest.MonkeyPatch, headers: dict[str, str]) -> None:
    _configure_managed_runtime(monkeypatch)

    with pytest.raises(HTTPException) as exc_info:
        resolve_run_aws_runtime(_request(headers), aws_managed=True, org_id=_ORG_ID)

    assert exc_info.value.status_code == 400
    assert "AWS request headers are not accepted" in exc_info.value.detail


def test_run_runtime_rejects_access_key_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run operations require a saved deployment-managed runtime."""
    _configure_managed_runtime(monkeypatch)

    with pytest.raises(HTTPException) as exc_info:
        resolve_run_aws_runtime(_request(), aws_managed=False, org_id=_ORG_ID)

    assert exc_info.value.status_code == 400
    assert "no deployment-managed AWS runtime" in exc_info.value.detail


def test_run_metadata_omits_runtime_for_access_key_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch, submissions_enabled=False)

    runtime = resolve_run_metadata_aws_runtime(_request(), aws_managed=False, org_id=_ORG_ID)

    assert runtime is None


def test_run_metadata_returns_deployment_runtime_for_managed_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch, submissions_enabled=False)

    runtime = resolve_run_metadata_aws_runtime(_request(), aws_managed=True, org_id=_ORG_ID)

    assert runtime is not None
    assert runtime.resources.s3_bucket == "deployment-bucket"


def test_managed_runtime_rejects_missing_deployment_account(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ACCOUNT_ID", None)

    with pytest.raises(HTTPException) as error:
        resolve_start_aws_runtime(_request(), _ORG_ID)

    assert error.value.status_code == 500
    assert error.value.detail == "AWS_DEPLOYMENT_ACCOUNT_ID must be a 12-digit AWS account ID"


def test_run_runtime_rejects_managed_run_for_ineligible_org(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch, eligible=False)

    with pytest.raises(HTTPException) as exc_info:
        resolve_run_aws_runtime(_request(), aws_managed=True, org_id=_ORG_ID)

    assert exc_info.value.status_code == 403


def test_saved_resources_survive_new_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resume uses the saved region and locations with the deployment credential source."""
    _configure_managed_runtime(monkeypatch)
    original = resolve_run_aws_runtime(_request(), aws_managed=True, org_id=_ORG_ID)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_REGION", "new-deployment-region")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_S3_BUCKET", "new-deployment-bucket")

    resumed = resolve_run_aws_runtime(
        _request(),
        aws_managed=True,
        org_id=_ORG_ID,
        properties=original.resources,
    )

    assert resumed.resources == original.resources
    assert isinstance(resumed.clients, DefaultChainAWSClientProvider)
    assert resumed.clients.region == original.resources.region


def test_managed_start_cannot_override_deployment_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    """Resource properties cannot give managed callers a different deployment bucket."""
    from dataclasses import replace

    _configure_managed_runtime(monkeypatch)
    original = resolve_start_aws_runtime(_request(), _ORG_ID)
    with pytest.raises(HTTPException) as error:
        resolve_start_aws_runtime(_request(), _ORG_ID, replace(original.resources, s3_bucket="other"))
    assert error.value.status_code == 400


@pytest.mark.parametrize(
    ("eligible", "submissions_enabled", "expected_mode"),
    [
        pytest.param(True, True, "managed", id="eligible-and-open"),
        pytest.param(True, False, "access_key", id="eligible-but-closed"),
        pytest.param(False, True, "access_key", id="ineligible"),
    ],
)
def test_aws_runtime_metadata_reflects_managed_submission_availability(
    monkeypatch: pytest.MonkeyPatch,
    eligible: bool,
    submissions_enabled: bool,
    expected_mode: str,
) -> None:
    _configure_managed_runtime(
        monkeypatch,
        eligible=eligible,
        submissions_enabled=submissions_enabled,
    )

    response = TestClient(app).get("/aws-runtime")

    assert response.status_code == 200
    assert response.json() == (
        {
            "mode": "managed",
            "region": "deployment-region",
            "s3_bucket": "deployment-bucket",
        }
        if expected_mode == "managed"
        else {"mode": "unavailable", "region": None, "s3_bucket": None}
    )


def _mapping_keys(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        for key, nested_value in cast(dict[object, object], value).items():
            yield str(key).lower().replace("-", "_")
            yield from _mapping_keys(nested_value)
    elif isinstance(value, list):
        for nested_value in cast(list[object], value):
            yield from _mapping_keys(nested_value)


def test_managed_execution_context_is_recursively_credential_free() -> None:
    request = RunExecutionRequest(
        contract=AgentContractRequest(
            name="test-agent",
            install_cmd="echo install",
            run_cmd="echo run",
            secrets={"STAGING_AWS_PROFILE": "profile-name"},
        ),
        benchmark_name="test-benchmark",
        sandbox_provider="daytona",
        sandbox_provider_secret_name="sandbox-provider-secret",
        service_headers={"Authorization": "benchmark-service-token"},
    )
    context = ManagedExecutionContext(
        version=2,
        benchmark_id=UUID("00000000-0000-0000-0000-000000000003"),
        verified_task_ids=["task-1"],
        start_benchmark_request=request,
    )

    payload = context.model_dump(mode="json")
    forbidden_keys = {
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "aws_profile",
    }
    assert forbidden_keys.isdisjoint(_mapping_keys(payload))

    for credential_bearing_request in (
        request.model_copy(update={"service_headers": {"X-Harness-Aws-Access-Key-Id": "credential"}}),
        request.model_copy(
            update={"contract": request.contract.model_copy(update={"secrets": {"aws_profile": "profile-secret-name"}})}
        ),
    ):
        with pytest.raises(ValidationError, match="Managed execution cannot include AWS credentials"):
            ManagedExecutionContext(
                version=2,
                benchmark_id=context.benchmark_id,
                verified_task_ids=context.verified_task_ids,
                start_benchmark_request=credential_bearing_request,
            )


def test_start_request_rejects_unsupported_fields() -> None:
    """Start requests enforce the documented payload schema."""
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        StartBenchmarkRequest(
            contract=AgentContractRequest(name="agent", run_cmd="run"),
            benchmark_name="test",
            harness_config={
                "aws": {
                    "aws_access_key_id": "key",
                    "aws_secret_access_key": "secret",
                    "aws_default_region": "region",
                },
                "s3_bucket": "bucket",
                "log_group": "logs",
                "log_retention_policy": 30,
                "sandbox_provider_secret_name": "secret-name",
            },
        )


def test_agent_list_uses_deployment_runtime_for_eligible_org(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch, submissions_enabled=False)
    from tracker.aws.s3 import S3ObjectStore

    create_store = MagicMock(wraps=S3ObjectStore)
    monkeypatch.setattr("tracker.aws.services.S3ObjectStore", create_store)
    monkeypatch.setattr("tracker.api.agents.list_agents", AsyncMock(return_value=[]))
    response = TestClient(app).get("/agents")

    assert response.status_code == 200
    create_store.assert_called_once()
    runtime = cast(AWSRuntime, create_store.call_args.args[0])
    assert runtime.resources.s3_bucket == "deployment-bucket"


def test_managed_results_report_capped_presign_expiry(
    monkeypatch: pytest.MonkeyPatch,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    _configure_managed_runtime(monkeypatch, submissions_enabled=False)
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


def _managed_start_request(**overrides: object) -> StartBenchmarkRequest:
    return StartBenchmarkRequest(
        contract=AgentContractRequest(name="agent", run_cmd="run"),
        benchmark_name="test",
        **cast(Any, overrides),
    )


@pytest.mark.parametrize("sandbox_provider", [None, "daytona", ""])
def test_managed_request_uses_deployment_sandbox_provider_default(
    monkeypatch: pytest.MonkeyPatch, sandbox_provider: str | None
) -> None:
    """A managed request without a provider secret resolves the deployment default pair."""
    _configure_managed_runtime(monkeypatch)
    request = _managed_start_request(sandbox_provider=sandbox_provider)

    resolved = resolve_managed_sandbox_provider(request)

    assert resolved.sandbox_provider == "daytona"
    assert resolved.sandbox_provider_secret_name == "deployment-provider-secret"


def test_managed_request_honors_explicit_provider_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit provider and secret pair is not rewritten to the deployment default."""
    _configure_managed_runtime(monkeypatch)
    request = _managed_start_request(sandbox_provider="modal", sandbox_provider_secret_name="ModalSecrets")

    resolved = resolve_managed_sandbox_provider(request)

    assert resolved.sandbox_provider == "modal"
    assert resolved.sandbox_provider_secret_name == "ModalSecrets"


def test_managed_request_rejects_unknown_provider_without_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    """Naming a non-default provider without a secret fails instead of switching providers."""
    _configure_managed_runtime(monkeypatch)
    request = _managed_start_request(sandbox_provider="modal")

    with pytest.raises(HTTPException) as error:
        resolve_managed_sandbox_provider(request)

    assert error.value.status_code == 400
    assert "no configured secret for sandbox provider 'modal'" in error.value.detail


def test_managed_request_requires_deployment_sandbox_provider_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """A deployment without provider defaults reports a configuration error, not a client error."""
    _configure_managed_runtime(monkeypatch)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME", None)
    request = _managed_start_request()

    with pytest.raises(HTTPException) as error:
        resolve_managed_sandbox_provider(request)

    assert error.value.status_code == 500
    assert "AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME" in error.value.detail


def test_managed_start_errors_do_not_direct_users_to_access_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hosted managed failures give supported recovery steps instead of AWS credential setup."""
    _configure_managed_runtime(monkeypatch, submissions_enabled=False)
    with pytest.raises(HTTPException) as error:
        resolve_start_aws_runtime(_request(), _ORG_ID)
    assert error.value.status_code == 503
    assert "access key" not in error.value.detail.lower()
    assert "Try again later or contact Vals support" in error.value.detail

    _configure_managed_runtime(monkeypatch, eligible=False)
    with pytest.raises(HTTPException) as error:
        resolve_start_aws_runtime(_request(), _ORG_ID)
    assert error.value.status_code == 403
    assert "access key" not in error.value.detail.lower()
    assert "Contact Vals support" in error.value.detail


def test_local_start_is_rejected_before_aws_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """The AWS-only stack must not silently accept local execution."""
    resolve = MagicMock(side_effect=AssertionError("AWS resolution should not run"))
    monkeypatch.setattr("main.resolve_start_aws_runtime", resolve)
    response = TestClient(app).post(
        "/start-benchmark",
        json={"environment": "local", "benchmark_name": "test", "contract": {"name": "agent", "run_cmd": "run"}},
    )
    assert response.status_code == 422
    resolve.assert_not_called()


@pytest.mark.parametrize("invalid_property", [{"region": ""}, {"s3_bucket": ""}, {"log_retention_days": 0}])
def test_start_rejects_invalid_resource_properties(invalid_property: dict[str, object]) -> None:
    """Reject unusable resource settings before admitting a run."""
    response = TestClient(app).post(
        "/start-benchmark",
        json={
            "benchmark_name": "test",
            "contract": {"name": "agent", "run_cmd": "run"},
            "properties": {
                "region": "us-east-1",
                "s3_bucket": "bucket",
                "log_group": "logs",
                "log_retention_days": 30,
                **invalid_property,
            },
        },
    )
    assert response.status_code == 422
