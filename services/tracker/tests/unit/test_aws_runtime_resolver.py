from collections.abc import Iterator
from dataclasses import replace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlmodel import Session

from main import app
from tracker import config
from tracker.aws.clients import AWSClientProvider, TenantRoleAWSClientProvider
from tracker.aws.resolver import (
    deployment_aws_runtime,
    resolve_run_aws_runtime,
    resolve_run_metadata_aws_runtime,
    resolve_start_aws_runtime,
)
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.database.models import AgentContractRequest, Benchmark, Org
from tracker.types import ManagedExecutionContext, RunExecutionRequest, StartBenchmarkRequest

_ORG_ID = UUID("00000000-0000-0000-0000-000000000001")
_OTHER_ORG_ID = UUID("00000000-0000-0000-0000-000000000002")
_ORG = Org(id=_ORG_ID, name="acme.example", storage_key="org-bucket")


def _configure_managed_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    submissions_enabled: bool = True,
    resources_configured: bool = True,
) -> None:
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ACCOUNT_ID", "123456789012")
    monkeypatch.setattr(config, "AWS_MANAGED_SUBMISSIONS_ENABLED", submissions_enabled)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_REGION", "deployment-region" if resources_configured else None)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_S3_BUCKET", "deployment-bucket" if resources_configured else None)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_GROUP", "deployment-log-group" if resources_configured else None)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "30" if resources_configured else None)


@pytest.mark.parametrize(
    ("submissions_enabled", "resources_configured", "expected_status"),
    [
        pytest.param(True, True, None, id="managed-open"),
        pytest.param(False, True, 503, id="managed-gate-closed"),
        pytest.param(True, False, 500, id="managed-config-missing"),
    ],
)
def test_start_runtime_selection(
    monkeypatch: pytest.MonkeyPatch,
    submissions_enabled: bool,
    resources_configured: bool,
    expected_status: int | None,
) -> None:
    _configure_managed_runtime(
        monkeypatch,
        submissions_enabled=submissions_enabled,
        resources_configured=resources_configured,
    )

    if expected_status is not None:
        with pytest.raises(HTTPException) as exc_info:
            resolve_start_aws_runtime(_ORG)
        assert exc_info.value.status_code == expected_status
        return

    runtime = resolve_start_aws_runtime(_ORG)

    assert runtime.resources.s3_bucket == "org-bucket"
    assert isinstance(runtime.clients, AWSClientProvider)
    assert runtime.expected_bucket_owner == "123456789012"


@pytest.mark.parametrize("log_prefix", ["/deployment/logs", "/deployment/logs/"])
def test_managed_runtime_uses_the_org_bucket_with_deployment_settings(
    monkeypatch: pytest.MonkeyPatch, log_prefix: str
) -> None:
    _configure_managed_runtime(monkeypatch)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_GROUP", log_prefix)

    runtime = resolve_start_aws_runtime(_ORG)

    assert runtime.resources == AWSResources(
        region="deployment-region",
        s3_bucket="org-bucket",
        log_group=f"/deployment/logs/tenants/{_ORG_ID}",
        log_retention_days=30,
    )


def test_new_run_logs_use_org_identity_not_its_name(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch)
    renamed_org = Org(id=_ORG_ID, name="renamed", storage_key="org-bucket")
    other_org = Org(id=_OTHER_ORG_ID, name=_ORG.name, storage_key="other-bucket")

    first = resolve_start_aws_runtime(_ORG)
    renamed = resolve_start_aws_runtime(renamed_org)
    other = resolve_start_aws_runtime(other_org)

    assert first.resources.log_group == f"deployment-log-group/tenants/{_ORG_ID}"
    assert renamed.resources.log_group == first.resources.log_group
    assert other.resources.log_group == f"deployment-log-group/tenants/{_OTHER_ORG_ID}"


def test_legacy_run_without_saved_resources_keeps_the_deployment_log_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch)

    runtime = resolve_run_aws_runtime(org=_ORG, properties=None)
    metadata = resolve_run_metadata_aws_runtime(org=_ORG, properties=None)

    assert runtime.resources.log_group == "deployment-log-group"
    assert metadata is not None
    assert metadata.resources.log_group == "deployment-log-group"


@pytest.mark.parametrize(
    "log_group",
    ["deployment-log-group", f"deployment-log-group/tenants/{_OTHER_ORG_ID}", "unrelated/logs"],
)
def test_new_run_cannot_select_logs_outside_its_tenant(monkeypatch: pytest.MonkeyPatch, log_group: str) -> None:
    _configure_managed_runtime(monkeypatch)
    resources = resolve_start_aws_runtime(_ORG).resources

    with pytest.raises(HTTPException) as error:
        resolve_start_aws_runtime(_ORG, replace(resources, log_group=log_group))

    assert error.value.status_code == 400
    assert resolve_start_aws_runtime(_ORG, resources).resources == resources


def test_run_saved_outside_the_org_bucket_is_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    """A run stored before its organization had its own bucket can no longer be read or resumed, and its page has no
    storage links.
    """
    _configure_managed_runtime(monkeypatch)
    saved = AWSResources(
        region="deployment-region",
        s3_bucket="deployment-bucket",
        log_group="deployment-log-group",
        log_retention_days=30,
    )

    with pytest.raises(HTTPException) as exc_info:
        resolve_run_aws_runtime(org=_ORG, properties=saved)
    assert exc_info.value.status_code == 410
    assert exc_info.value.detail == "This run's storage is no longer available."
    assert resolve_run_metadata_aws_runtime(org=_ORG, properties=saved) is None


def test_hosted_org_without_a_bucket_cannot_use_managed_aws(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch)
    monkeypatch.setattr(config, "AUTH_REQUIRED", True)

    with pytest.raises(HTTPException) as exc_info:
        resolve_start_aws_runtime(Org(id=_ORG_ID, name="acme.example"))

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "This organization has no storage bucket"


def test_self_hosted_org_uses_the_deployment_bucket(monkeypatch: pytest.MonkeyPatch) -> None:
    """A Tracker without authentication runs one organization from its configured bucket."""
    _configure_managed_runtime(monkeypatch)
    monkeypatch.setattr(config, "AUTH_REQUIRED", False)

    runtime = resolve_start_aws_runtime(Org(id=_ORG_ID, name="default"))

    assert runtime.resources.s3_bucket == "deployment-bucket"


def test_run_saved_in_the_org_bucket_uses_its_saved_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    """A run saved in the organization's bucket keeps its saved resources, such as a log group from its own config.

    Test cases:
    - The runtime uses the saved resources with deployment credentials and the deployment's bucket owner.
    - Metadata reports the saved bucket.
    """
    _configure_managed_runtime(monkeypatch)
    saved = AWSResources(region="us-east-1", s3_bucket="org-bucket", log_group="benchmarks", log_retention_days=7)

    runtime = resolve_run_aws_runtime(org=_ORG, properties=saved)
    metadata = resolve_run_metadata_aws_runtime(org=_ORG, properties=saved)

    assert runtime.resources == saved
    assert isinstance(runtime.clients, AWSClientProvider)
    assert runtime.expected_bucket_owner == "123456789012"
    assert metadata is not None and metadata.resources == saved


def test_run_metadata_returns_deployment_runtime_for_managed_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch, submissions_enabled=False)

    runtime = resolve_run_metadata_aws_runtime(org=_ORG)

    assert runtime is not None
    assert runtime.resources.s3_bucket == "org-bucket"


def test_managed_runtime_rejects_missing_deployment_account(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ACCOUNT_ID", None)

    with pytest.raises(HTTPException) as error:
        resolve_start_aws_runtime(_ORG)

    assert error.value.status_code == 500
    assert error.value.detail == "AWS_DEPLOYMENT_ACCOUNT_ID must be a 12-digit AWS account ID"


def test_saved_resources_survive_new_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_managed_runtime(monkeypatch)
    original = resolve_run_aws_runtime(org=_ORG)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_REGION", "new-deployment-region")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_S3_BUCKET", "new-deployment-bucket")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_LOG_GROUP", "new-deployment-logs")

    resumed = resolve_run_aws_runtime(
        org=_ORG,
        properties=original.resources,
    )

    assert resumed.resources == original.resources
    assert isinstance(resumed.clients, AWSClientProvider)
    assert resumed.clients.region == original.resources.region


def test_managed_start_cannot_override_deployment_resources(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch)
    original = resolve_start_aws_runtime(_ORG)
    with pytest.raises(HTTPException) as error:
        resolve_start_aws_runtime(_ORG, replace(original.resources, s3_bucket="other"))
    assert error.value.status_code == 400


@pytest.mark.parametrize(
    ("submissions_enabled", "expected_status"),
    [
        pytest.param(True, 200, id="open"),
        pytest.param(False, 503, id="closed"),
    ],
)
def test_aws_runtime_metadata_reflects_managed_submission_availability(
    monkeypatch: pytest.MonkeyPatch,
    submissions_enabled: bool,
    expected_status: int,
) -> None:
    _configure_managed_runtime(monkeypatch, submissions_enabled=submissions_enabled)

    response = TestClient(app).get("/aws-runtime")

    assert response.status_code == expected_status
    if expected_status == 200:
        assert response.json() == {"mode": "managed", "region": "deployment-region", "s3_bucket": "test-bucket"}


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


def test_start_request_ignores_retired_fields() -> None:
    request = StartBenchmarkRequest.model_validate(
        {
            "contract": {"name": "agent", "run_cmd": "run"},
            "benchmark_name": "test",
            "harness_config": None,
            "webhook_secret_name": None,
            "webhook_intervals": None,
        }
    )

    assert {"harness_config", "webhook_secret_name", "webhook_intervals"}.isdisjoint(request.model_dump())


def test_agent_list_uses_the_org_bucket_for_eligible_org(
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
    assert runtime.resources.s3_bucket == "test-bucket"


def test_managed_results_report_capped_presign_expiry(
    monkeypatch: pytest.MonkeyPatch,
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    _configure_managed_runtime(monkeypatch, submissions_enabled=False)
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


def test_managed_start_errors_provide_recovery_steps(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch, submissions_enabled=False)
    with pytest.raises(HTTPException) as error:
        resolve_start_aws_runtime(_ORG)
    assert error.value.status_code == 503
    assert "Try again later or contact Vals support" in error.value.detail


def test_local_start_is_rejected_before_aws_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """The AWS-only stack must not silently accept local execution."""
    resolve = MagicMock(side_effect=AssertionError("AWS resolution should not run"))
    monkeypatch.setattr("main.resolve_start_aws_runtime", resolve)
    response = TestClient(app).post(
        "/start-benchmark",
        json={"environment": "local", "benchmark_name": "test", "contract": {"name": "agent", "run_cmd": "run"}},
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "Server has no local configuration"
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


def test_hosted_runtime_uses_the_default_chain_without_a_tenant_role(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure_managed_runtime(monkeypatch)
    monkeypatch.setattr(config, "AWS_TENANT_ACCESS_ROLE_ARN", None)

    assert type(deployment_aws_runtime(_ORG).clients) is AWSClientProvider


@pytest.mark.parametrize(
    ("shared_secret_tenants", "shared_secrets"),
    [("", False), ("acme.*", True)],
    ids=["own-secrets", "shared-secrets"],
)
def test_hosted_runtime_assumes_the_tenant_role_for_its_org(
    monkeypatch: pytest.MonkeyPatch, shared_secret_tenants: str, shared_secrets: bool
) -> None:
    _configure_managed_runtime(monkeypatch)
    role_arn = "arn:aws:iam::123456789012:role/ValkyrieTrackerTenantAccessRole-dev"
    monkeypatch.setattr(config, "AWS_TENANT_ACCESS_ROLE_ARN", role_arn)
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", shared_secret_tenants)

    clients = deployment_aws_runtime(_ORG).clients

    assert clients == TenantRoleAWSClientProvider(
        region="deployment-region", role_arn=role_arn, org_id=str(_ORG_ID), shared_secrets=shared_secrets
    )


@pytest.mark.parametrize("environment", ["dev", "prod"])
def test_dedicated_customer_runtime_cannot_assume_shared_secret_scope(
    monkeypatch: pytest.MonkeyPatch, environment: str
) -> None:
    _configure_managed_runtime(monkeypatch)
    monkeypatch.setattr(config, "AWS_TENANT_ACCESS_ROLE_ARN", "arn:aws:iam::123456789012:role/TenantAccess")
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "vals.ai,vals-smith-*")
    customer = Org(id=_ORG_ID, name=f"vals-smith-{environment}-gh-52346350", storage_key="customer-bucket")

    clients = deployment_aws_runtime(customer).clients

    assert isinstance(clients, TenantRoleAWSClientProvider)
    assert clients.shared_secrets is False
    assert clients.org_id == str(_ORG_ID)
