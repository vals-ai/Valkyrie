from uuid import UUID

import pytest
from fastapi import HTTPException

from tracker.aws.credentials import aws_client_kwargs
from tracker.aws.s3 import get_agent_result_s3_key, get_contract_s3_key
from tracker.database.models import AgentContractRequest, Benchmark, BenchmarkArguments, Org
from tracker.exceptions import TrackerServiceError
from tracker.runtime import (
    LegacyRuntime,
    harness_config_for_benchmark,
    harness_config_for_runtime,
    resolve_runtime,
    runtime_locator,
)
from tracker.types import AWSCredentials, HarnessConfig, TaskRoleAWSConfig
from tracker.utils.resources import fetch_sandbox_provider_config


def test_managed_runtime_isolates_two_orgs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tracker.config.AUTH_REQUIRED", True)
    monkeypatch.setattr("tracker.config.MANAGED_RUNTIME_SANDBOX_PROVIDER_SECRET_NAME", "provider-secret")

    first = harness_config_for_runtime(resolve_runtime(Org(id=UUID(int=1), name="first"), None, None))
    second = harness_config_for_runtime(resolve_runtime(Org(id=UUID(int=2), name="second"), None, None))

    assert first.s3_bucket == second.s3_bucket
    assert first.s3_prefix == "orgs/00000000000000000000000000000001"
    assert second.s3_prefix == "orgs/00000000000000000000000000000002"
    assert get_contract_s3_key("agent", first.s3_prefix) != get_contract_s3_key("agent", second.s3_prefix)
    assert get_agent_result_s3_key("run", "task", "output", first.s3_prefix) != get_agent_result_s3_key(
        "run", "task", "output", second.s3_prefix
    )
    assert first.log_group != second.log_group


def test_explicit_runtime_preserves_legacy_config(harness_config: HarnessConfig) -> None:
    runtime = resolve_runtime(Org(id=UUID(int=1), name="first"), harness_config, None)

    assert isinstance(runtime, LegacyRuntime)
    assert harness_config_for_runtime(runtime) is harness_config
    assert harness_config.s3_prefix == ""
    assert get_contract_s3_key("agent") == "agents/agent.zip"
    assert get_agent_result_s3_key("run", "task", "output") == "benchmarks/run/task/output"


def test_task_role_aws_config_omits_static_credentials() -> None:
    aws = TaskRoleAWSConfig(aws_default_region="us-east-1")

    assert aws_client_kwargs(aws) == {"region_name": "us-east-1"}


def test_managed_provider_uses_injected_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tracker.config.MANAGED_RUNTIME_SANDBOX_PROVIDER_SECRET_NAME", "provider-secret")
    monkeypatch.setattr(
        "tracker.config.MANAGED_RUNTIME_SANDBOX_PROVIDER_CONFIG",
        '{"DAYTONA_API_KEY":"key","DAYTONA_API_URL":"https://example.com","DAYTONA_TARGET":"target"}',
    )

    provider = fetch_sandbox_provider_config(
        "provider-secret",
        TaskRoleAWSConfig(aws_default_region="us-east-1"),
        "daytona",
    )

    assert provider.model_dump(mode="json") == {
        "type": "daytona",
        "DAYTONA_API_KEY": "key",
        "DAYTONA_API_URL": "https://example.com",
        "DAYTONA_TARGET": "target",
    }


def test_managed_provider_validation_never_exposes_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "do-not-log-this-key"
    monkeypatch.setattr("tracker.config.AUTH_REQUIRED", True)
    monkeypatch.setattr("tracker.config.MANAGED_RUNTIME_SANDBOX_PROVIDER_SECRET_NAME", "provider-secret")
    monkeypatch.setattr(
        "tracker.config.MANAGED_RUNTIME_SANDBOX_PROVIDER_CONFIG",
        f'{{"DAYTONA_API_KEY":"{secret}"}}',
    )

    with pytest.raises(TrackerServiceError) as provider_error:
        fetch_sandbox_provider_config(
            "provider-secret",
            TaskRoleAWSConfig(aws_default_region="us-east-1"),
            "daytona",
        )
    with pytest.raises(HTTPException) as readiness_error:
        resolve_runtime(Org(id=UUID(int=1), name="first"), None, None)

    assert secret not in str(provider_error.value)
    assert provider_error.value.args == ("Managed runtime sandbox provider config is invalid",)
    assert readiness_error.value.status_code == 503
    assert secret not in str(readiness_error.value)


@pytest.mark.parametrize("gateway_url", ["http://[", "https://gateway.example:99999"])
def test_invalid_gateway_url_has_sanitized_readiness_error(
    gateway_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("tracker.config.AUTH_REQUIRED", True)
    monkeypatch.setattr("tracker.config.MODEL_GATEWAY_URL", gateway_url)

    with pytest.raises(HTTPException) as error:
        resolve_runtime(Org(id=UUID(int=1), name="first"), None, None)

    assert error.value.status_code == 503
    assert error.value.detail == "Managed Valkyrie runtime is not ready"
    assert gateway_url not in str(error.value)


def test_managed_run_uses_persisted_runtime_after_legacy_rollback(harness_config: HarnessConfig) -> None:
    org = Org(id=UUID(int=1), name="first")
    managed = HarnessConfig(
        aws=TaskRoleAWSConfig(aws_default_region="us-east-1"),
        s3_bucket="managed-bucket",
        s3_prefix=f"orgs/{org.id.hex}",
        log_group=f"managed-logs/orgs/{org.id.hex}",
        log_retention_policy=30,
        sandbox_provider_secret_name="managed-provider",
    )
    benchmark = Benchmark(
        org_id=org.id,
        name="swebench",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="agent"),
            concurrency=1,
            runtime=runtime_locator(managed),
        ),
    )

    resolved = harness_config_for_benchmark(benchmark, harness_config, org)

    assert isinstance(resolved.aws, TaskRoleAWSConfig)
    assert resolved.s3_bucket == "managed-bucket"
    assert resolved.s3_prefix == f"orgs/{org.id.hex}"


def test_legacy_run_requires_preserved_credentials_after_managed_cutover(harness_config: HarnessConfig) -> None:
    org = Org(id=UUID(int=1), name="first")
    legacy = harness_config.model_copy(update={"s3_bucket": "original-bucket"})
    benchmark = Benchmark(
        org_id=org.id,
        name="swebench",
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name="agent"),
            concurrency=1,
            runtime=runtime_locator(legacy),
        ),
    )
    managed_request = legacy.model_copy(update={"aws": TaskRoleAWSConfig(aws_default_region="us-east-1")})

    with pytest.raises(HTTPException, match="preserved self-hosted runtime config"):
        harness_config_for_benchmark(benchmark, managed_request, org)

    changed_legacy_request = legacy.model_copy(
        update={
            "aws": AWSCredentials(
                aws_access_key_id="new-key",
                aws_secret_access_key="new-secret",
                aws_default_region="us-west-2",
            ),
            "s3_bucket": "wrong-bucket",
        }
    )
    resolved = harness_config_for_benchmark(benchmark, changed_legacy_request, org)
    assert resolved.s3_bucket == "original-bucket"
    assert isinstance(resolved.aws, AWSCredentials)
    assert resolved.aws.aws_access_key_id == "new-key"
