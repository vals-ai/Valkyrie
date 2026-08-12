"""Per-stage infrastructure configuration."""

from __future__ import annotations

from dataclasses import dataclass

from aws_cdk import aws_logs
from stage import DEV, PROD, RELEASE_TEST, Stage


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
class StageConfig:
    runtime_environment: str
    tracker: ServiceConfig
    worker: ServiceConfig
    database: DatabaseConfig
    service_log_retention: aws_logs.RetentionDays


PROD_CONFIG = StageConfig(
    runtime_environment="production",
    tracker=ServiceConfig(cpu=4096, memory_mib=8192, min_tasks=1, max_tasks=2),
    worker=ServiceConfig(cpu=8192, memory_mib=32768, min_tasks=4, max_tasks=8),
    database=DatabaseConfig(
        instance_class="t4g.small",
        allocated_storage_gb=20,
        backup_retention_days=7,
        connection_alarm_threshold=135,
    ),
    service_log_retention=aws_logs.RetentionDays.ONE_YEAR,
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
)

RELEASE_TEST_CONFIG = StageConfig(
    runtime_environment=RELEASE_TEST,
    tracker=DEV_CONFIG.tracker,
    worker=DEV_CONFIG.worker,
    database=DEV_CONFIG.database,
    service_log_retention=DEV_CONFIG.service_log_retention,
)

RELEASE_TEST_BENCHMARK_SERVICE_BASE_URL = "benchmarks.vals.ai"


_STAGE_CONFIGS = {
    PROD: PROD_CONFIG,
    DEV: DEV_CONFIG,
    RELEASE_TEST: RELEASE_TEST_CONFIG,
}


def config_for(stage: Stage) -> StageConfig:
    try:
        return _STAGE_CONFIGS[stage.name]
    except KeyError:
        raise ValueError(f"unknown stage {stage.name!r}; expected {PROD!r}, {DEV!r}, or 'release-test'") from None


def benchmark_service_base_url(stage: Stage) -> str | None:
    """Return the externally reachable benchmark-service base URL for isolated stages."""
    if not stage.is_release_test:
        return None
    return RELEASE_TEST_BENCHMARK_SERVICE_BASE_URL
