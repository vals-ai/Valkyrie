"""Per-stage infrastructure configuration."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import cast
from uuid import UUID

from aws_cdk import aws_logs
from stage import BENCH, DEV, PROD, RELEASE_TEST, Stage


_SECRET_NAME_PREFIX_PATTERN = re.compile(r"[A-Za-z0-9/_+=.@-]+")
_LAMBDA_FUNCTION_NAME_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*\*?")
_KMS_KEY_ARN_PATTERN = re.compile(r"arn:[^:]+:kms:[^:]+:[0-9]{12}:key/[A-Za-z0-9-]+")
_OFFLINE_SYNTH_ORG_ID = "00000000-0000-0000-0000-000000000001"
_OFFLINE_SYNTH_SECRET_PREFIX = "offline-synth"
_MANAGED_STORAGE_ENVIRONMENTS = frozenset({"dev", "prod"})

_TRACKER_ANALYZER_LAMBDA_PATTERNS = ("analysis-*",)
_EXECUTOR_OUTPUT_LAMBDA_PATTERNS = (
    "vals-format-lambda",
    "harvey-legal-agent-final-view-lambda",
    "programbench-final-view-lambda",
    "snap-final-view-lambda",
    "swebench-final-view-lambda",
    "terminalbench-final-view-lambda",
)
_TRACKER_LAMBDA_PATTERNS = _TRACKER_ANALYZER_LAMBDA_PATTERNS + _EXECUTOR_OUTPUT_LAMBDA_PATTERNS


def _empty_managed_storage_environment_mapping() -> Mapping[UUID, frozenset[str]]:
    return MappingProxyType({})


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
    managed_storage_org_environments: Mapping[UUID, frozenset[str]] = field(
        default_factory=_empty_managed_storage_environment_mapping
    )
    managed_storage_submissions_enabled: bool = False
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
                parsed_org_id = UUID(org_id.strip())
            except ValueError:
                raise ValueError(f"deployment_role_org_ids contains an invalid UUID: {org_id!r}") from None
            if org_id != str(parsed_org_id):
                raise ValueError(f"deployment_role_org_ids must contain canonical UUIDs: {org_id!r}")

        eligible_org_ids = {UUID(org_id) for org_id in self.deployment_role_org_ids}
        managed_storage_org_environments: dict[UUID, frozenset[str]] = {}
        for org_id, environments in self.managed_storage_org_environments.items():
            if org_id not in eligible_org_ids:
                raise ValueError("managed_storage_org_environments must contain only deployment_role_org_ids")

            if not environments or not environments.issubset(_MANAGED_STORAGE_ENVIRONMENTS):
                raise ValueError("managed_storage_org_environments values must be non-empty subsets of dev and prod")

            managed_storage_org_environments[org_id] = frozenset(environments)

        object.__setattr__(
            self,
            "managed_storage_org_environments",
            MappingProxyType(managed_storage_org_environments),
        )

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

    @property
    def managed_storage_org_environments_json(self) -> str:
        return json.dumps(
            {
                str(org_id): sorted(environments)
                for org_id, environments in self.managed_storage_org_environments.items()
            },
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True)
class StageConfig:
    runtime_environment: str
    sentry_environment: str
    tracker: ServiceConfig
    worker: ServiceConfig
    database: DatabaseConfig
    service_log_retention: aws_logs.RetentionDays
    managed_aws: ManagedAWSRuntimeConfig


BENCH_CONFIG = StageConfig(
    runtime_environment="production",
    sentry_environment=BENCH,
    tracker=ServiceConfig(cpu=4096, memory_mib=8192, min_tasks=1, max_tasks=2),
    worker=ServiceConfig(cpu=8192, memory_mib=32768, min_tasks=4, max_tasks=8),
    database=DatabaseConfig(
        instance_class="r7g.large",
        allocated_storage_gb=20,
        backup_retention_days=7,
        connection_alarm_threshold=1400,
    ),
    service_log_retention=aws_logs.RetentionDays.ONE_YEAR,
    managed_aws=ManagedAWSRuntimeConfig(
        benchmark_log_group_prefix="/valkyrie/benchmarks",
        benchmark_log_retention_days=365,
        submissions_enabled=True,
        executor_all_secret_access=True,
        tracker_lambda_function_name_patterns=_TRACKER_LAMBDA_PATTERNS,
        executor_lambda_function_name_patterns=_EXECUTOR_OUTPUT_LAMBDA_PATTERNS,
    ),
)

PROD_CONFIG = StageConfig(
    runtime_environment="production",
    sentry_environment="production",
    tracker=ServiceConfig(cpu=4096, memory_mib=8192, min_tasks=1, max_tasks=2),
    worker=ServiceConfig(cpu=8192, memory_mib=32768, min_tasks=4, max_tasks=8),
    database=DatabaseConfig(
        instance_class="r7g.large",
        allocated_storage_gb=20,
        backup_retention_days=7,
        connection_alarm_threshold=1400,
    ),
    service_log_retention=aws_logs.RetentionDays.ONE_YEAR,
    managed_aws=ManagedAWSRuntimeConfig(
        benchmark_log_group_prefix="/valkyrie/benchmarks",
        benchmark_log_retention_days=365,
        submissions_enabled=True,
        executor_all_secret_access=True,
        tracker_lambda_function_name_patterns=("vals-format-lambda",),
        executor_lambda_function_name_patterns=("vals-format-lambda",),
    ),
)

DEV_CONFIG = StageConfig(
    runtime_environment="dev",
    sentry_environment=DEV,
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
        tracker_lambda_function_name_patterns=_TRACKER_LAMBDA_PATTERNS,
        executor_lambda_function_name_patterns=_EXECUTOR_OUTPUT_LAMBDA_PATTERNS,
    ),
)

RELEASE_TEST_CONFIG = StageConfig(
    runtime_environment=RELEASE_TEST,
    sentry_environment=RELEASE_TEST,
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
    BENCH: BENCH_CONFIG,
    PROD: PROD_CONFIG,
    DEV: DEV_CONFIG,
    RELEASE_TEST: RELEASE_TEST_CONFIG,
}


def config_for(stage: Stage) -> StageConfig:
    try:
        config = _STAGE_CONFIGS[stage.name]
    except KeyError:
        raise ValueError(
            f"unknown stage {stage.name!r}; expected {BENCH!r}, {PROD!r}, {DEV!r}, or 'release-test'"
        ) from None

    if stage.name not in {BENCH, DEV, PROD}:
        return config

    deployment_role_org_ids = _csv_environment("AWS_DEPLOYMENT_ROLE_ORG_IDS")
    tracker_secret_name_prefixes = _csv_environment("AWS_TRACKER_SECRET_NAME_PREFIXES")
    if os.environ.get("DESCOPE_PROJECT_ID") == _OFFLINE_SYNTH_SECRET_PREFIX:
        deployment_role_org_ids = deployment_role_org_ids or (_OFFLINE_SYNTH_ORG_ID,)
        tracker_secret_name_prefixes = tracker_secret_name_prefixes or (_OFFLINE_SYNTH_SECRET_PREFIX,)
    if config.managed_aws.submissions_enabled:
        if not deployment_role_org_ids:
            raise ValueError(f"{stage.name} deployments require AWS_DEPLOYMENT_ROLE_ORG_IDS.")
        if not tracker_secret_name_prefixes:
            raise ValueError(f"{stage.name} deployments require AWS_TRACKER_SECRET_NAME_PREFIXES.")

    managed_storage_org_environments = _managed_storage_environment_mapping(
        "AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS",
        deployment_role_org_ids,
    )
    managed_storage_submissions_enabled = _boolean_environment(
        "AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED",
        default=False,
    )

    if managed_storage_submissions_enabled and not managed_storage_org_environments:
        raise ValueError(
            f"{stage.name} deployments require AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS "
            "when AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED is true."
        )

    return replace(
        config,
        managed_aws=replace(
            config.managed_aws,
            deployment_role_org_ids=deployment_role_org_ids,
            managed_storage_org_environments=managed_storage_org_environments,
            managed_storage_submissions_enabled=managed_storage_submissions_enabled,
            tracker_secret_name_prefixes=tracker_secret_name_prefixes,
        ),
    )


def _csv_environment(name: str) -> tuple[str, ...]:
    return tuple(value.strip() for value in os.environ.get(name, "").split(",") if value.strip())


def _managed_storage_environment_mapping(
    name: str,
    deployment_role_org_ids: tuple[str, ...],
) -> Mapping[UUID, frozenset[str]]:
    try:
        raw_mapping = json.loads(os.environ.get(name, "{}"))
        if not isinstance(raw_mapping, dict):
            raise ValueError

        eligible_org_ids = {UUID(org_id) for org_id in deployment_role_org_ids}
        mapping: dict[UUID, frozenset[str]] = {}
        for raw_org_id, raw_environments in cast(dict[object, object], raw_mapping).items():
            if not isinstance(raw_org_id, str) or not isinstance(raw_environments, list):
                raise ValueError

            org_id = UUID(raw_org_id)
            if raw_org_id != str(org_id) or org_id not in eligible_org_ids:
                raise ValueError

            environments = cast(list[object], raw_environments)
            if (
                not environments
                or any(not isinstance(environment, str) for environment in environments)
                or len(environments) != len(set(environments))
                or not set(environments).issubset(_MANAGED_STORAGE_ENVIRONMENTS)
            ):
                raise ValueError

            mapping[org_id] = frozenset(cast(list[str], environments))
    except (TypeError, ValueError, json.JSONDecodeError):
        raise ValueError(
            f"{name} must be a JSON object from managed organization UUIDs to non-empty dev/prod lists"
        ) from None

    return MappingProxyType(mapping)


def _boolean_environment(name: str, *, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default

    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be 'true' or 'false'")

    return value == "true"


def benchmark_service_base_url(stage: Stage) -> str | None:
    """Return the externally reachable benchmark-service base URL for isolated stages."""
    if not stage.is_release_test:
        return None
    return RELEASE_TEST_BENCHMARK_SERVICE_BASE_URL
