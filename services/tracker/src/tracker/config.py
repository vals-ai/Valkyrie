"""Configuration for the tracker service."""

import os
from typing import Any

from dotenv import load_dotenv
from taskiq import InMemoryBroker
from taskiq_redis import RedisStreamBroker
from taskiq_redis.redis_backend import RedisAsyncResultBackend

from tracker.logging import configure_logging
from tracker.middleware import LoggingContextMiddleware, TaskProtectionMiddleware

load_dotenv()
configure_logging()


_BENCHMARK_SERVICE_NAMESPACE: str = "local"
_BENCHMARK_SERVICE_PORT = 8001


def create_benchmark_service_url(benchmark_name: str) -> str:
    """
    Derive the benchmark service URL from the benchmark name and namespace

    NOTE: If we are running this locally the namespace is blank
    """
    host = f"{benchmark_name}.{_BENCHMARK_SERVICE_NAMESPACE}"
    return f"http://{host}:{_BENCHMARK_SERVICE_PORT}"


AWS_S3_BUCKET = os.environ.get("AWS_S3_BUCKET", "agentic-harness")
BROKER_ENVIRONMENT = os.environ.get("BROKER_ENVIRONMENT", "production")
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

broker = (
    InMemoryBroker()
    if BROKER_ENVIRONMENT == "testing"
    else RedisStreamBroker(
        url=REDIS_URL,
        idle_timeout=86400000,  # 24 hours
    )
    .with_result_backend(result_backend)
    .with_middlewares(TaskProtectionMiddleware(), LoggingContextMiddleware())
)

# Auth settings
AUTH_REQUIRED = os.environ.get("AUTH_REQUIRED", "false").lower() == "true"
DESCOPE_PROJECT_ID = os.environ.get("DESCOPE_PROJECT_ID", "")
