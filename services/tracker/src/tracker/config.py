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

load_dotenv()
configure_logging()


_BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE: str = os.environ.get("BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE", "local")
_CLOUDMAP_PORT = 8001
_BENCHMARK_SERVICE_BASE_URL: str | None = os.environ.get("BENCHMARK_SERVICE_BASE_URL")


def create_benchmark_service_url(benchmark_name: str) -> str:
    """
    Derive the benchmark service URL from the benchmark name.

    NOTE: If BENCHMARK_SERVICE_BASE_URL is set (e.g. benchmarks.vals.ai), use HTTPS subdomains.
    Otherwise fall back to CloudMap internal DNS (only works inside the VPC).
    """
    if _BENCHMARK_SERVICE_BASE_URL:
        return f"https://{benchmark_name}.{_BENCHMARK_SERVICE_BASE_URL}"

    return f"http://{benchmark_name}.{_BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE}:{_CLOUDMAP_PORT}"


AWS_S3_BUCKET = os.environ.get("AWS_S3_BUCKET", "agentic-harness")
BROKER_ENVIRONMENT = os.environ.get("BROKER_ENVIRONMENT", "production")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")


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
