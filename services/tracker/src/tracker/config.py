"""Configuration for the tracker service."""

import os

from dotenv import load_dotenv

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
