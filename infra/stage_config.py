"""Per-stage infrastructure configuration."""

from __future__ import annotations

from dataclasses import dataclass

from aws_cdk import aws_logs
from stage import DEV, PROD, Stage

SHARED_DESCOPE_PROJECT_ID = "P2ktNOjz5Tgzs9wwS3VpShnCbmik"


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
    benchmark_service_access_key_secret_prefix: str
    sandbox_provider: str
    sandbox_provider_secret_name: str
    managed_tenant_ids: tuple[str, ...] = ("vals.ai",)
    submissions_enabled: bool = False
    worker_secret_names: tuple[str, ...] = ()
    tracker_secret_name_prefixes: tuple[str, ...] = ()
    worker_secret_name_prefixes: tuple[str, ...] = ()
    tracker_lambda_function_name_patterns: tuple[str, ...] = ()
    worker_lambda_function_name_patterns: tuple[str, ...] = ()
    kms_key_arns: tuple[str, ...] = ()


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
    tracker=ServiceConfig(cpu=1024, memory_mib=2048, min_tasks=1, max_tasks=2),
    worker=ServiceConfig(cpu=4096, memory_mib=8192, min_tasks=2, max_tasks=4),
    database=DatabaseConfig(
        instance_class="t4g.small",
        allocated_storage_gb=20,
        backup_retention_days=7,
        connection_alarm_threshold=135,
    ),
    service_log_retention=aws_logs.RetentionDays.ONE_YEAR,
    managed_aws=ManagedAWSRuntimeConfig(
        benchmark_log_group_prefix="/valkyrie/benchmarks",
        benchmark_log_retention_days=365,
        benchmark_service_access_key_secret_prefix="valkyrie/benchmark-service-access-key",
        sandbox_provider="daytona",
        sandbox_provider_secret_name="",
        managed_tenant_ids=("vals.ai",),
    ),
)

DEV_CONFIG = StageConfig(
    runtime_environment="dev",
    tracker=ServiceConfig(cpu=1024, memory_mib=2048, min_tasks=1, max_tasks=1),
    worker=ServiceConfig(cpu=4096, memory_mib=8192, min_tasks=1, max_tasks=2),
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
        benchmark_service_access_key_secret_prefix="valkyrie/benchmark-service-access-key",
        sandbox_provider="daytona",
        sandbox_provider_secret_name="",
        managed_tenant_ids=("vals.ai",),
    ),
)

_STAGE_CONFIGS = {
    PROD: PROD_CONFIG,
    DEV: DEV_CONFIG,
}


def config_for(stage: Stage) -> StageConfig:
    try:
        return _STAGE_CONFIGS[stage.name]
    except KeyError:
        raise ValueError(f"unknown stage {stage.name!r}; expected {PROD!r} or {DEV!r}") from None
