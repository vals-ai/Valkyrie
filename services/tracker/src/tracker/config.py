"""Configuration for the tracker service."""

from enum import Enum
import os
from typing import Any
from urllib.parse import urlsplit

from dotenv import load_dotenv
from executor_protocol import DEFAULT_STABLE_QUEUE_NAME
from taskiq import InMemoryBroker, TaskiqEvents
from taskiq_redis import RedisStreamBroker
from taskiq_redis.redis_backend import RedisAsyncResultBackend

from tracker.logging import configure_logging
from tracker.middleware import LoggingContextMiddleware, TracingContextMiddleware
from tracker.observability import configure_observability
from tracker.outbound_security import validate_benchmark_name

load_dotenv()
configure_logging()


_BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE: str = os.environ.get("BENCHMARK_SERVICE_CLOUDMAP_NAMESPACE", "local")
_CLOUDMAP_PORT = 8001
_BENCHMARK_SERVICE_BASE_URL: str | None = os.environ.get("BENCHMARK_SERVICE_BASE_URL")
BENCHMARK_CATALOG_URL = os.environ.get("BENCHMARK_CATALOG_URL", "").rstrip("/")


class BenchmarkServiceDestination(Enum):
    """Trust classification for benchmark-service credential forwarding."""

    HOSTED = "hosted"
    CUSTOM = "custom"


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


def _normalized_origin(url: str) -> tuple[str, str, int]:
    parsed = urlsplit(url)
    assert parsed.hostname is not None
    default_port = 443 if parsed.scheme == "https" else 80
    port = parsed.port if parsed.port is not None else default_port
    return parsed.scheme, parsed.hostname.lower(), port


def classify_benchmark_service_destination(
    benchmark_name: str,
    service_url: str | None,
) -> BenchmarkServiceDestination:
    """Trust only the exact origin derived for a hosted benchmark service."""
    try:
        hosted_url = create_benchmark_service_url(benchmark_name)
    except ValueError:
        # Persisted custom-service runs can predate benchmark-name validation.
        if service_url is not None:
            return BenchmarkServiceDestination.CUSTOM
        raise
    effective_url = service_url or hosted_url
    if _normalized_origin(effective_url) == _normalized_origin(hosted_url):
        return BenchmarkServiceDestination.HOSTED
    return BenchmarkServiceDestination.CUSTOM


AWS_S3_BUCKET = os.environ.get("AWS_S3_BUCKET", "agentic-harness")
AWS_DEPLOYMENT_ROLE_ORG_IDS = os.environ.get("AWS_DEPLOYMENT_ROLE_ORG_IDS", "")
AWS_DEPLOYMENT_REGION = os.environ.get("AWS_DEPLOYMENT_REGION") or os.environ.get("AWS_REGION")
AWS_DEPLOYMENT_S3_BUCKET = os.environ.get("AWS_DEPLOYMENT_S3_BUCKET")
AWS_DEPLOYMENT_LOG_GROUP = os.environ.get("AWS_DEPLOYMENT_LOG_GROUP")
AWS_DEPLOYMENT_LOG_RETENTION_DAYS = os.environ.get("AWS_DEPLOYMENT_LOG_RETENTION_DAYS")
AWS_MANAGED_SUBMISSIONS_ENABLED = os.environ.get("AWS_MANAGED_SUBMISSIONS_ENABLED", "false").lower() == "true"
BROKER_ENVIRONMENT = os.environ.get("BROKER_ENVIRONMENT", "production")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
STABLE_QUEUE_NAME = os.environ.get("STABLE_QUEUE_NAME", DEFAULT_STABLE_QUEUE_NAME)


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
_BROKER_MIDDLEWARES = (TracingContextMiddleware(), LoggingContextMiddleware())

broker = (
    InMemoryBroker().with_middlewares(*_BROKER_MIDDLEWARES)
    if BROKER_ENVIRONMENT == "testing"
    else RedisStreamBroker(
        url=REDIS_URL,
        queue_name=STABLE_QUEUE_NAME,
        consumer_group_name=STABLE_QUEUE_NAME,
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
