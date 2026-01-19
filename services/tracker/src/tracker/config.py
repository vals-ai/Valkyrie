"""Configuration for the tracker service."""

import os
from typing import Any

from dotenv import load_dotenv
from taskiq import InMemoryBroker
from taskiq_redis import RedisStreamBroker
from taskiq_redis.redis_backend import RedisAsyncResultBackend

load_dotenv()


def get_benchmark_service_url() -> str:
    url = os.getenv("BENCHMARK_SERVICE_URL")
    if not url:
        raise ValueError("BENCHMARK_SERVICE_URL environment variable is not set")
    return url


def get_s3_bucket_name() -> str:
    bucket_name = os.getenv("AWS_S3_BUCKET")
    if not bucket_name:
        raise ValueError("AWS_S3_BUCKET environment variable is not set")
    return bucket_name


def get_redis_url() -> str:
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        raise ValueError("REDIS_URL environment variable is not set")
    return redis_url


BROKER_ENVIRONMENT = os.environ.get("BROKER_ENVIRONMENT", "production")

result_backend: RedisAsyncResultBackend[Any] = RedisAsyncResultBackend(
    redis_url=get_redis_url(),
)

if BROKER_ENVIRONMENT == "testing":
    broker = InMemoryBroker()
else:
    broker = RedisStreamBroker(
        url=get_redis_url(),
    ).with_result_backend(result_backend)
