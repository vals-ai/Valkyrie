"""Per-stage infrastructure configuration."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, replace
from uuid import UUID

from aws_cdk import aws_logs
from stage import DEV, PROD, RELEASE_TEST, Stage


_SECRET_NAME_PREFIX_PATTERN = re.compile(r"[A-Za-z0-9/_+=.@-]+")
_LAMBDA_FUNCTION_NAME_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*\*?")
_KMS_KEY_ARN_PATTERN = re.compile(r"arn:[^:]+:kms:[^:]+:[0-9]{12}:key/[A-Za-z0-9-]+")
_OFFLINE_SYNTH_ORG_ID = "00000000-0000-0000-0000-000000000001"
_OFFLINE_SYNTH_SECRET_PREFIX = "offline-synth"


@dataclass(frozen=True)
class ServiceConfig:
    cpu: int
    memory_mib: int
    min_tasks: int
    max_tasks: int


@dataclass(frozen=True)
class DatabaseConfig:
    instance_class: str
    allocated_storage_gb: int
    backup_retention_days: int
    connection_alarm_threshold: int


@dataclass(frozen=True)
class ManagedAWSRuntimeConfig:
    benchmark_log_group_prefix: str
    benchmark_log_retention_days: int
    deployment_role_org_ids: tuple[str, ...] = ()
    submissions_enabled: bool = False
    tracker_secret_name_prefixes: tuple[str, ...] = ()
    executor_secret_name_prefixes: tuple[str, ...] = ()
    executor_all_secret_access: bool = False
    tracker_lambda_function_name_patterns: tuple[str, ...] = ()
    executor_lambda_function_name_patterns: tuple[str, ...] = ()
    kms_key_arns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.benchmark_log_retention_days <= 0:
            raise ValueError("benchmark_log_retention_days must be positive")

        for org_id in self.deployment_role_org_ids:
            try:
                UUID(org_id.strip())
            except ValueError:
                raise ValueError(f"deployment_role_org_ids contains an invalid UUID: {org_id!r}") from None

        for field_name, prefixes in (
            ("tracker_secret_name_prefixes", self.tracker_secret_name_prefixes),
            ("executor_secret_name_prefixes", self.executor_secret_name_prefixes),
        ):
            if any(_SECRET_NAME_PREFIX_PATTERN.fullmatch(prefix) is None for prefix in prefixes):
                raise ValueError(f"{field_name} must contain literal, non-empty Secrets Manager name prefixes")

        if self.executor_all_secret_access and self.executor_secret_name_prefixes:
            raise ValueError("executor_all_secret_access cannot be combined with executor_secret_name_prefixes")

        for field_name, patterns in (
            ("tracker_lambda_function_name_patterns", self.tracker_lambda_function_name_patterns),
            ("executor_lambda_function_name_patterns", self.executor_lambda_function_name_patterns),
        ):
            if any(_LAMBDA_FUNCTION_NAME_PATTERN.fullmatch(pattern) is None for pattern in patterns):
                raise ValueError(
                    f"{field_name} must contain anchored Lambda function names or trailing-wildcard patterns"
                )

        if any(_KMS_KEY_ARN_PATTERN.fullmatch(arn) is None or "*" in arn or "?" in arn for arn in self.kms_key_arns):
            raise ValueError("kms_key_arns must contain concrete KMS key ARNs")


@dataclass(frozen=True)
class StageConfig:
    runtime_environment: str
    tracker: ServiceConfig
    worker: ServiceConfig
    database: DatabaseConfig
    service_log_retention: aws_logs.RetentionDays
    managed_aws: ManagedAWSRuntimeConfig


PROD_CONFIG = StageConfig(
    runtime_environment="production",
    tracker=ServiceConfig(cpu=4096, memory_mib=8192, min_tasks=1, max_tasks=2),
    worker=ServiceConfig(cpu=8192, memory_mib=32768, min_tasks=4, max_tasks=8),
    database=DatabaseConfig(
        instance_class="r7g.large",
        allocated_storage_gb=20,
        backup_retention_days=7,
        connection_alarm_threshold=135,
    ),
    service_log_retention=aws_logs.RetentionDays.ONE_YEAR,
    managed_aws=ManagedAWSRuntimeConfig(
        benchmark_log_group_prefix="/valkyrie/benchmarks",
        benchmark_log_retention_days=365,
    ),
)

DEV_CONFIG = StageConfig(
    runtime_environment="dev",
    tracker=ServiceConfig(cpu=4096, memory_mib=8192, min_tasks=1, max_tasks=2),
    worker=ServiceConfig(cpu=8192, memory_mib=32768, min_tasks=4, max_tasks=8),
    database=DatabaseConfig(
        instance_class="t4g.micro",
        allocated_storage_gb=20,
        backup_retention_days=1,
        connection_alarm_threshold=65,
    ),
    service_log_retention=aws_logs.RetentionDays.ONE_WEEK,
    managed_aws=ManagedAWSRuntimeConfig(
        benchmark_log_group_prefix="/valkyrie/benchmarks",
        benchmark_log_retention_days=7,
        submissions_enabled=True,
        executor_all_secret_access=True,
    ),
)

RELEASE_TEST_CONFIG = StageConfig(
    runtime_environment=RELEASE_TEST,
    tracker=DEV_CONFIG.tracker,
    worker=DEV_CONFIG.worker,
    database=DEV_CONFIG.database,
    service_log_retention=DEV_CONFIG.service_log_retention,
    managed_aws=ManagedAWSRuntimeConfig(
        benchmark_log_group_prefix="/valkyrie/benchmarks",
        benchmark_log_retention_days=7,
    ),
)

RELEASE_TEST_BENCHMARK_SERVICE_BASE_URL = "benchmarks.vals.ai"


_STAGE_CONFIGS = {
    PROD: PROD_CONFIG,
    DEV: DEV_CONFIG,
    RELEASE_TEST: RELEASE_TEST_CONFIG,
}


def config_for(stage: Stage) -> StageConfig:
    try:
        config = _STAGE_CONFIGS[stage.name]
    except KeyError:
        raise ValueError(f"unknown stage {stage.name!r}; expected {PROD!r}, {DEV!r}, or 'release-test'") from None

    if stage.name != DEV:
        return config

    deployment_role_org_ids = _csv_environment("AWS_DEPLOYMENT_ROLE_ORG_IDS")
    tracker_secret_name_prefixes = _csv_environment("AWS_TRACKER_SECRET_NAME_PREFIXES")
    if os.environ.get("DESCOPE_PROJECT_ID") == _OFFLINE_SYNTH_SECRET_PREFIX:
        deployment_role_org_ids = deployment_role_org_ids or (_OFFLINE_SYNTH_ORG_ID,)
        tracker_secret_name_prefixes = tracker_secret_name_prefixes or (_OFFLINE_SYNTH_SECRET_PREFIX,)
    if config.managed_aws.submissions_enabled:
        if not deployment_role_org_ids:
            raise ValueError("Development deployments require AWS_DEPLOYMENT_ROLE_ORG_IDS.")
        if not tracker_secret_name_prefixes:
            raise ValueError("Development deployments require AWS_TRACKER_SECRET_NAME_PREFIXES.")
    return replace(
        config,
        managed_aws=replace(
            config.managed_aws,
            deployment_role_org_ids=deployment_role_org_ids,
            tracker_secret_name_prefixes=tracker_secret_name_prefixes,
        ),
    )


def _csv_environment(name: str) -> tuple[str, ...]:
    return tuple(value.strip() for value in os.environ.get(name, "").split(",") if value.strip())


def benchmark_service_base_url(stage: Stage) -> str | None:
    """Return the externally reachable benchmark-service base URL for isolated stages."""
    if not stage.is_release_test:
        return None
    return RELEASE_TEST_BENCHMARK_SERVICE_BASE_URL
