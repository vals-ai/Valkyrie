"""Configuration for the tracker service."""

import os

from dotenv import load_dotenv

load_dotenv()

BENCHMARK_SERVICE_URL = os.getenv("BENCHMARK_SERVICE_URL", "http://localhost:8002")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "agentic-harness")
