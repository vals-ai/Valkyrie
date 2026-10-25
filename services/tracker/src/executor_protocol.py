"""Shared executor payload protocol."""

from collections.abc import Mapping
from enum import Enum
from pathlib import Path
from typing import TypedDict, cast

RESERVED_QUEUE_PROTOCOL_VERSION = "5"
SUPPORTED_PROTOCOL_VERSION = RESERVED_QUEUE_PROTOCOL_VERSION
SUPPORTED_PROTOCOL_VERSIONS = frozenset({"1", "2", "3", "4", SUPPORTED_PROTOCOL_VERSION})
MANAGED_EXECUTION_PROTOCOL_VERSION = "3"
DATASET_VERSION_PROTOCOL_VERSION = "4"


class ExecutorDispatchStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class ExecutorTelemetryContext(TypedDict):
    request_id: str
    trace_headers: dict[str, str]


def normalize_executor_telemetry_context(value: object) -> ExecutorTelemetryContext:
    """Normalize optional telemetry fields at the executor wire boundary."""
    if not isinstance(value, Mapping):
        return {"request_id": "", "trace_headers": {}}

    telemetry_context = cast(Mapping[object, object], value)
    request_id = telemetry_context.get("request_id")
    raw_trace_headers = telemetry_context.get("trace_headers")
    trace_headers = (
        {str(key): str(header) for key, header in cast(Mapping[object, object], raw_trace_headers).items()}
        if isinstance(raw_trace_headers, Mapping)
        else {}
    )

    return {
        "request_id": str(request_id) if request_id else "",
        "trace_headers": trace_headers,
    }


def validate_executor_digest(digest: str) -> str:
    normalized = digest.lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError("Executor artifact digest must be a 64-character SHA-256 digest")
    return normalized


def validate_executor_image_uri(uri: str, expected_repository: str) -> str:
    """Require an image release to pin the configured ECR repository by digest; return the digest."""
    repository, separator, digest = uri.partition("@sha256:")
    if not separator or repository != expected_repository:
        raise ValueError("Executor image must pin the configured ECR repository by digest")
    return validate_executor_digest(digest)


def source_executor_artifact_uri(root: Path) -> str:
    """Name a checkout's executor source tree as a release artifact."""
    return root.as_uri().replace("file://", "source://", 1)
