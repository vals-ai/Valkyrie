"""Shared Tracker-to-ExecutorHost wire contract."""

from enum import Enum
from typing import Any, TypedDict, Unpack
from urllib.parse import urlparse

EXECUTOR_TASK_NAME = "tracker.utils:process_benchmark"
SUPPORTED_PROTOCOL_VERSION = "1"
DEFAULT_STABLE_QUEUE_NAME = "valkyrie-stable"
DEFAULT_EXECUTOR_RELEASE_PREFIX = "releases"


class ExecutorDispatchStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class ExecutorPayload(TypedDict):
    start_benchmark_request_json: dict[str, Any]
    benchmark_id_str: str
    verified_task_ids: list[str]
    executor_dispatch_id: str
    executor_release_id: str
    executor_artifact_uri: str
    executor_artifact_digest: str
    executor_protocol_version: str


async def executor_task_signature(**_payload: Unpack[ExecutorPayload]) -> None:
    """Provide the producer's typed Taskiq signature; this body never executes."""
    raise RuntimeError("Executor task signatures cannot execute in Tracker")


def validate_executor_digest(digest: str) -> str:
    normalized = digest.lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError("Executor artifact digest must be a 64-character SHA-256 digest")
    return normalized


def validate_executor_artifact_uri(uri: str, expected_bucket: str, expected_prefix: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    key = parsed.path.lstrip("/")
    prefix = expected_prefix.strip("/")
    if parsed.scheme != "s3" or not parsed.netloc or not key:
        raise ValueError("Executor artifact URI must identify an S3 object")
    if parsed.netloc != expected_bucket or not prefix or not key.startswith(f"{prefix}/"):
        raise ValueError("Executor artifact URI is outside the configured S3 bucket and prefix")
    return parsed.netloc, key
