"""Configuration for the tracker service."""

import os
from typing import Any

from dotenv import load_dotenv
from taskiq import InMemoryBroker
from taskiq_redis import RedisStreamBroker
from taskiq_redis.redis_backend import RedisAsyncResultBackend

load_dotenv()

BENCHMARK_SERVICE_URL = os.environ.get("BENCHMARK_SERVICE_URL", "http://localhost:8001")
AWS_S3_BUCKET = os.environ.get("AWS_S3_BUCKET", "agentic-harness")
BROKER_ENVIRONMENT = os.environ.get("BROKER_ENVIRONMENT", "production")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

result_backend: RedisAsyncResultBackend[Any] = RedisAsyncResultBackend(
    redis_url=REDIS_URL,
)

broker = (
    InMemoryBroker()
    if BROKER_ENVIRONMENT == "testing"
    else RedisStreamBroker(url=REDIS_URL).with_result_backend(result_backend)
)
