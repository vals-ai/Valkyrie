"""Release validation and external artifact verification orchestration."""

import hashlib
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from urllib.parse import urlparse
from uuid import UUID

import boto3

from executor_protocol import (
    SUPPORTED_PROTOCOL_VERSION,
    validate_executor_artifact_uri,
    validate_executor_digest,
)
from tracker.database.models import Benchmark, ExecutorDispatch, ExecutorDispatchKind, ExecutorRelease
from tracker.database.repositories import ExecutorControlRepository
from tracker.exceptions import ReleaseControlError

_logger = logging.getLogger(__name__)


class S3Body(Protocol):
    def read(self, size: int = -1) -> bytes: ...

    def close(self) -> None: ...


class S3Client(Protocol):
    def get_object(self, *, Bucket: str, Key: str) -> Mapping[str, object]: ...


@dataclass(frozen=True)
class ReleaseSnapshot:
    """Immutable release identity copied into a dispatch row."""

    release_id: str
    artifact_uri: str
    artifact_digest: str
    protocol_version: str


def validate_release_manifest(release: ExecutorRelease) -> None:
    """Validate and normalize a release manifest without database or network work."""
    try:
        release.artifact_digest = validate_executor_digest(release.artifact_digest)
    except ValueError as error:
        raise ReleaseControlError(str(error)) from error
    if release.protocol_version != SUPPORTED_PROTOCOL_VERSION:
        raise ReleaseControlError(f"Unsupported executor protocol version: {release.protocol_version}")
    if not release.artifact_uri.startswith("s3://"):
        raise ReleaseControlError("Executor artifact URI must use s3://")


def _validate_release_identity(existing: ExecutorRelease, candidate: ExecutorRelease) -> None:
    if (
        existing.artifact_uri != candidate.artifact_uri
        or existing.artifact_digest != candidate.artifact_digest
        or existing.protocol_version != candidate.protocol_version
    ):
        raise ReleaseControlError(f"Executor release {candidate.id!r} already has a different immutable identity")
    if existing.status.value in ("DRAINING", "RETIRED"):
        raise ReleaseControlError(f"Executor release {existing.id!r} cannot be activated from {existing.status.value}")


def prepare_release_activation(
    repository: ExecutorControlRepository,
    candidate: ExecutorRelease,
    *,
    expected_bucket: str,
    expected_prefix: str,
) -> ExecutorRelease:
    """Validate and stage a release candidate in a short database phase."""
    validate_release_manifest(candidate)
    try:
        validate_executor_artifact_uri(candidate.artifact_uri, expected_bucket, expected_prefix)
    except ValueError as error:
        raise ReleaseControlError(str(error)) from error
    repository.get_executor_admission(for_update=True)
    release = repository.find_release(candidate.id)
    if release is None:
        release = repository.register_release(candidate)
    else:
        _validate_release_identity(release, candidate)
    return release


def verify_release_artifact(
    release: ExecutorRelease,
    *,
    s3_client: S3Client | None = None,
) -> dict[str, Any]:
    """Verify an external artifact without holding a database transaction."""
    if release.status.value == "RETIRED":
        raise ReleaseControlError(f"Retired executor release {release.id!r} cannot be verified")
    parsed = urlparse(release.artifact_uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ReleaseControlError("Executor artifact URI must use s3://bucket/key")

    client: S3Client
    if s3_client is None:
        boto3_client: Any = boto3.client("s3")  # pyright: ignore
        client = cast(S3Client, boto3_client)
    else:
        client = s3_client
    response = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    body = cast(S3Body, response["Body"])
    digest = hashlib.sha256()
    artifact_bytes = 0
    try:
        while chunk := body.read(1024 * 1024):
            digest.update(chunk)
            artifact_bytes += len(chunk)
    finally:
        body.close()
    if digest.hexdigest() != release.artifact_digest:
        raise ReleaseControlError(f"Executor release {release.id!r} artifact digest mismatch")
    return {"artifact_bytes": artifact_bytes, "verified_at": datetime.now(UTC).isoformat()}


def complete_release_activation(
    repository: ExecutorControlRepository,
    candidate: ExecutorRelease,
    verification_metadata: dict[str, Any],
) -> ExecutorRelease:
    """Stage readiness and promote a prepared release in a fresh database phase."""
    repository.get_executor_admission(for_update=True)
    release = repository.find_release(candidate.id)
    if release is None:
        raise ReleaseControlError(f"Prepared executor release {candidate.id!r} does not exist")
    _validate_release_identity(release, candidate)
    repository.mark_release_verified(release.id, verification_metadata)
    activated = repository.promote_release(release.id)
    admission = repository.get_executor_admission()
    if admission.release_id != activated.id or activated.status.value != "ACTIVE" or not activated.readiness_verified:
        raise ReleaseControlError(f"Executor release {activated.id!r} did not become active")
    _logger.info(
        "executor_release_lifecycle",
        extra={"event": "verified", "release_id": activated.id, "status": activated.status.value},
    )
    return activated


def pin_benchmark_to_release(benchmark: Benchmark, release: ExecutorRelease) -> None:
    """Apply the pure release identity fields to a benchmark model."""
    if benchmark.executor_release_id is not None:
        raise ReleaseControlError(f"Benchmark {benchmark.id} already has executor release ownership")
    benchmark.executor_release_id = release.id
    benchmark.current_execution_release_id = release.id
    benchmark.executor_artifact_uri = release.artifact_uri
    benchmark.executor_artifact_digest = release.artifact_digest
    benchmark.executor_protocol_version = release.protocol_version


def create_executor_dispatch(
    benchmark_id: UUID,
    release: ExecutorRelease,
    kind: ExecutorDispatchKind,
    *,
    dispatch_id: UUID,
) -> ExecutorDispatch:
    """Construct an immutable release snapshot for a queued dispatch."""
    return ExecutorDispatch(
        id=dispatch_id,
        benchmark_id=benchmark_id,
        kind=kind,
        executor_release_id=release.id,
        executor_artifact_uri=release.artifact_uri,
        executor_artifact_digest=release.artifact_digest,
        executor_protocol_version=release.protocol_version,
    )


def release_snapshot(release: ExecutorRelease) -> ReleaseSnapshot:
    """Return a pure immutable release snapshot for callers that need one."""
    return ReleaseSnapshot(
        release_id=release.id,
        artifact_uri=release.artifact_uri,
        artifact_digest=release.artifact_digest,
        protocol_version=release.protocol_version,
    )
