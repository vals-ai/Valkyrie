"""Configuration for the tracker service."""

import os
from typing import Any

from dotenv import load_dotenv
from taskiq import InMemoryBroker
from taskiq_redis import RedisStreamBroker
from taskiq_redis.redis_backend import RedisAsyncResultBackend

load_dotenv()

BENCHMARK_SERVICE_URL = os.getenv("BENCHMARK_SERVICE_URL", "http://localhost:8002")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "agentic-harness")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
BROKER_ENVIRONMENT = os.environ.get("BROKER_ENVIRONMENT", "production")

result_backend: RedisAsyncResultBackend[Any] = RedisAsyncResultBackend(
    redis_url=REDIS_URL,
)

if BROKER_ENVIRONMENT == "testing":
    broker = InMemoryBroker()
else:
    broker = RedisStreamBroker(
        url=REDIS_URL,
    ).with_result_backend(result_backend)
