"""Resolve caller-owned and hosted Tracker runtimes."""

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from fastapi import HTTPException

from tracker import config
from tracker.database.models import (
    Benchmark,
    LegacyRunRuntimeLocator,
    ManagedRunRuntimeLocator,
    Org,
    RunRuntimeLocator,
)
from tracker.exceptions import TrackerServiceError
from tracker.model_gateway import model_gateway_origin
from tracker.types import AWSCredentials, HarnessConfig, TaskRoleAWSConfig


@dataclass(frozen=True)
class LegacyRuntime:
    kind: Literal["legacy"]
    harness_config: HarnessConfig


@dataclass(frozen=True)
class ManagedRuntime:
    kind: Literal["managed"]
    org_id: UUID
    aws_region: str
    s3_bucket: str
    log_group: str
    log_retention_policy: int
    sandbox_provider_secret_name: str


Runtime = LegacyRuntime | ManagedRuntime


def validate_managed_runtime() -> None:
    if not (
        config.MANAGED_RUNTIME_SANDBOX_PROVIDER_SECRET_NAME
        and config.MANAGED_RUNTIME_SANDBOX_PROVIDER_CONFIG
        and config.BENCHMARK_CATALOG_URL
        and config.MODEL_GATEWAY_URL
        and len(config.VALKYRIE_GATEWAY_SIGNING_KEY.encode()) >= 32
    ):
        raise HTTPException(status_code=503, detail="Managed Valkyrie runtime is not configured")

    from tracker.utils.resources import fetch_sandbox_provider_config

    try:
        model_gateway_origin()
        fetch_sandbox_provider_config(
            config.MANAGED_RUNTIME_SANDBOX_PROVIDER_SECRET_NAME,
            TaskRoleAWSConfig(aws_default_region=config.MANAGED_RUNTIME_AWS_REGION),
            config.MANAGED_RUNTIME_SANDBOX_PROVIDER,
        )
    except TrackerServiceError:
        raise HTTPException(status_code=503, detail="Managed Valkyrie runtime is not ready") from None


def managed_runtime(org: Org) -> ManagedRuntime:
    validate_managed_runtime()
    runtime = ManagedRuntime(
        kind="managed",
        org_id=org.id,
        aws_region=config.MANAGED_RUNTIME_AWS_REGION,
        s3_bucket=config.AWS_S3_BUCKET,
        log_group=config.MANAGED_RUNTIME_LOG_GROUP,
        log_retention_policy=config.MANAGED_RUNTIME_LOG_RETENTION_POLICY,
        sandbox_provider_secret_name=config.MANAGED_RUNTIME_SANDBOX_PROVIDER_SECRET_NAME,
    )
    return runtime


def harness_config_for_runtime(runtime: Runtime) -> HarnessConfig:
    if isinstance(runtime, LegacyRuntime):
        return runtime.harness_config

    org_prefix = f"orgs/{runtime.org_id.hex}"
    return HarnessConfig(
        aws=TaskRoleAWSConfig(aws_default_region=runtime.aws_region),
        s3_bucket=runtime.s3_bucket,
        s3_prefix=org_prefix,
        log_group=f"{runtime.log_group}/{org_prefix}",
        log_retention_policy=runtime.log_retention_policy,
        sandbox_provider_secret_name=runtime.sandbox_provider_secret_name,
    )


def runtime_locator(harness_config: HarnessConfig) -> RunRuntimeLocator:
    if isinstance(harness_config.aws, TaskRoleAWSConfig):
        return ManagedRunRuntimeLocator(
            aws_default_region=harness_config.aws.aws_default_region,
            s3_bucket=harness_config.s3_bucket,
            s3_prefix=harness_config.s3_prefix,
            log_group=harness_config.log_group,
            log_retention_policy=harness_config.log_retention_policy,
            sandbox_provider_secret_name=harness_config.sandbox_provider_secret_name,
        )
    return LegacyRunRuntimeLocator(
        aws_default_region=harness_config.aws.aws_default_region,
        s3_bucket=harness_config.s3_bucket,
        s3_prefix=harness_config.s3_prefix,
        log_group=harness_config.log_group,
        log_retention_policy=harness_config.log_retention_policy,
        sandbox_provider_secret_name=harness_config.sandbox_provider_secret_name,
    )


def harness_config_for_benchmark(
    benchmark: Benchmark,
    request_config: HarnessConfig,
    org: Org,
) -> HarnessConfig:
    """Resolve an existing run against the runtime where it was created."""
    locator = benchmark.arguments.runtime
    if locator is None:
        if isinstance(request_config.aws, TaskRoleAWSConfig):
            raise HTTPException(status_code=409, detail="Legacy run requires the preserved self-hosted runtime config")
        return request_config

    if isinstance(locator, ManagedRunRuntimeLocator):
        assert locator.s3_prefix == f"orgs/{org.id.hex}"
        aws = TaskRoleAWSConfig(aws_default_region=locator.aws_default_region)
    else:
        if not isinstance(request_config.aws, AWSCredentials):
            raise HTTPException(status_code=409, detail="Legacy run requires the preserved self-hosted runtime config")
        aws = request_config.aws.model_copy(update={"aws_default_region": locator.aws_default_region})

    return HarnessConfig(
        aws=aws,
        s3_bucket=locator.s3_bucket,
        s3_prefix=locator.s3_prefix,
        log_group=locator.log_group,
        log_retention_policy=locator.log_retention_policy,
        sandbox_provider_secret_name=locator.sandbox_provider_secret_name,
    )


def resolve_runtime(
    org: Org,
    header_harness_config: HarnessConfig | None,
    body_harness_config: HarnessConfig | None,
    managed: bool = False,
) -> Runtime:
    if managed:
        if not config.AUTH_REQUIRED:
            raise HTTPException(status_code=400, detail="Managed runtime requires hosted authentication")
        return managed_runtime(org)
    if header_harness_config:
        return LegacyRuntime(kind="legacy", harness_config=header_harness_config)
    if body_harness_config:
        if isinstance(body_harness_config.aws, TaskRoleAWSConfig):
            raise HTTPException(status_code=400, detail="Managed runtime requires the explicit runtime header")
        return LegacyRuntime(kind="legacy", harness_config=body_harness_config)
    if config.AUTH_REQUIRED:
        return managed_runtime(org)

    raise HTTPException(status_code=400, detail="Missing harness config")
