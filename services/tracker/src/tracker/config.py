"""Configuration for the tracker service."""

import os
from typing import Any

from dotenv import load_dotenv
from taskiq import InMemoryBroker, TaskiqEvents
from taskiq_redis import RedisStreamBroker
from taskiq_redis.redis_backend import RedisAsyncResultBackend

from tracker.logging import configure_logging
from tracker.middleware import LoggingContextMiddleware, TaskProtectionMiddleware, TracingContextMiddleware
from tracker.observability import configure_observability
from tracker.outbound_security import validate_benchmark_name

load_dotenv()
configure_logging()


_BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE: str = os.environ.get("BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE", "local")
_CLOUDMAP_PORT = 8001
_BENCHMARK_SERVICE_BASE_URL: str | None = os.environ.get("BENCHMARK_SERVICE_BASE_URL")
BENCHMARK_CATALOG_URL = os.environ.get("BENCHMARK_CATALOG_URL", "").rstrip("/")


def create_benchmark_service_url(benchmark_name: str) -> str:
    """
    Derive the benchmark service URL from the benchmark name.

    NOTE: If BENCHMARK_SERVICE_BASE_URL is set (e.g. benchmarks.vals.ai), use HTTPS subdomains.
    Otherwise fall back to CloudMap internal DNS (only works inside the VPC).
    """
    benchmark_name = validate_benchmark_name(benchmark_name)

    if _BENCHMARK_SERVICE_BASE_URL:
        return f"https://{benchmark_name}.{_BENCHMARK_SERVICE_BASE_URL}"

    return f"http://{benchmark_name}.{_BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE}:{_CLOUDMAP_PORT}"


AWS_S3_BUCKET = os.environ.get("AWS_S3_BUCKET", "agentic-harness")
AWS_MANAGED_TENANT_IDS = os.environ.get("AWS_MANAGED_TENANT_IDS", "")
AWS_DEPLOYMENT_REGION = os.environ.get("AWS_DEPLOYMENT_REGION") or os.environ.get("AWS_REGION")
AWS_DEPLOYMENT_S3_BUCKET = os.environ.get("AWS_DEPLOYMENT_S3_BUCKET")
AWS_DEPLOYMENT_LOG_GROUP = os.environ.get("AWS_DEPLOYMENT_LOG_GROUP")
AWS_DEPLOYMENT_LOG_RETENTION_DAYS = os.environ.get("AWS_DEPLOYMENT_LOG_RETENTION_DAYS")
AWS_DEPLOYMENT_SANDBOX_PROVIDER = os.environ.get("AWS_DEPLOYMENT_SANDBOX_PROVIDER", "")
AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME = os.environ.get("AWS_DEPLOYMENT_SANDBOX_PROVIDER_SECRET_NAME", "")
AWS_MANAGED_AGENT_SECRET_NAMES = os.environ.get("AWS_MANAGED_AGENT_SECRET_NAMES", "")
AWS_MANAGED_SUBMISSIONS_ENABLED = os.environ.get("AWS_MANAGED_SUBMISSIONS_ENABLED", "false").lower() == "true"
BENCHMARK_SERVICE_ACCESS_KEY_SECRET_PREFIX = os.environ.get("BENCHMARK_SERVICE_ACCESS_KEY_SECRET_PREFIX", "")
BROKER_ENVIRONMENT = os.environ.get("BROKER_ENVIRONMENT", "production")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")


def managed_tenant_ids() -> frozenset[str]:
    return frozenset(value.strip() for value in AWS_MANAGED_TENANT_IDS.split(",") if value.strip())


def _build_database_url() -> str:
    """Build DATABASE_URL from individual components or use direct URL."""
    if url := os.environ.get("DATABASE_URL"):
        return url
    # Build from individual components (used with RDS secrets)
    db_user = os.environ.get("DB_USERNAME", "tracker")
    db_pass = os.environ.get("DB_PASSWORD", "tracker")
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "tracker")
    return f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"


DATABASE_URL = _build_database_url()

result_backend: RedisAsyncResultBackend[Any] = RedisAsyncResultBackend(
    redis_url=REDIS_URL,
)

# Tracing precedes Logging so that anything emitted after (logs, child spans
# from middlewares or the task body) is captured under the propagated parent trace.
_BROKER_MIDDLEWARES = (TaskProtectionMiddleware(), TracingContextMiddleware(), LoggingContextMiddleware())

broker = (
    InMemoryBroker().with_middlewares(*_BROKER_MIDDLEWARES)
    if BROKER_ENVIRONMENT == "testing"
    else RedisStreamBroker(
        url=REDIS_URL,
        idle_timeout=86400000,  # 24 hours
    )
    .with_result_backend(result_backend)
    .with_middlewares(*_BROKER_MIDDLEWARES)
)


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def _init_worker_observability(*_args: object, **_kwargs: object) -> None:  # pyright: ignore[reportUnusedFunction]
    configure_observability("valkyrie-worker", environment=ENVIRONMENT)


AUTH_REQUIRED = os.environ.get("AUTH_REQUIRED", "false").lower() == "true"
DESCOPE_PROJECT_ID = os.environ.get("DESCOPE_PROJECT_ID", "")
DESCOPE_MANAGEMENT_KEY = os.environ.get("DESCOPE_MANAGEMENT_KEY", "")
