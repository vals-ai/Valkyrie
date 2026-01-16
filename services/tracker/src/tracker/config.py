"""Configuration for the tracker service."""

import os

from celery import Celery
from celery.app.task import Task
from dotenv import load_dotenv

load_dotenv()

BENCHMARK_SERVICE_URL = os.getenv("BENCHMARK_SERVICE_URL", "http://localhost:8002")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "agentic-harness")


# Monkey patch Task to allow generic params for celery-types https://pypi.org/project/celery-types/
Task.__class_getitem__ = classmethod(lambda cls, *args, **kwargs: cls)  # type: ignore[attr-defined]

# Celery configuration
# NOTE: Using a local redis instance but needs to be changed to a cloud redis instance
celery: Celery = Celery(
    "tracker",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)
