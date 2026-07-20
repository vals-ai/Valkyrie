"""Lifecycle operations for immutable executor releases in PostgreSQL."""

import hashlib
import logging
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast
from urllib.parse import urlparse
from uuid import UUID

import boto3
from sqlmodel import Session, col, func, select

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    ExecutorReleaseStatus,
)

_ACTIVE_BENCHMARK_STATUSES = (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING, BenchmarkStatus.STOPPED)
_ACTIVE_DISPATCH_STATUSES = (ExecutorDispatchStatus.QUEUED, ExecutorDispatchStatus.RUNNING)
_SUPPORTED_PROTOCOL_VERSION = "1"
_ARTIFACT_RETENTION_DAYS = 30
_logger = logging.getLogger(__name__)


class S3Body(Protocol):
    def read(self, size: int = -1) -> bytes: ...

    def close(self) -> None: ...


class S3Client(Protocol):
    def get_object(self, *, Bucket: str, Key: str) -> Mapping[str, object]: ...


class ReleaseControlError(ValueError):
    """Raised when a release lifecycle transition violates ownership rules."""


def register_release(session: Session, release: ExecutorRelease) -> ExecutorRelease:
    """Register a new immutable candidate release."""
    if session.get(ExecutorRelease, release.id) is not None:
        raise ReleaseControlError(f"Executor release {release.id!r} already exists")
    if release.status != ExecutorReleaseStatus.CANDIDATE:
        raise ReleaseControlError("New executor releases must start as candidates")
    _validate_release_manifest(release)

    session.add(release)
    session.flush()
    _logger.info(
        "executor_release_lifecycle",
        extra={"event": "registered", "release_id": release.id, "status": release.status.value},
    )
    return release


def verify_release_artifact(
    session: Session,
    release_id: str,
    *,
    s3_client: S3Client | None = None,
) -> ExecutorRelease:
    """Verify a registered release artifact and mark it ready for promotion."""
    release = _get_release(session, release_id)
    if release.status == ExecutorReleaseStatus.RETIRED:
        raise ReleaseControlError(f"Retired executor release {release_id!r} cannot be verified")
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
        raise ReleaseControlError(f"Executor artifact digest mismatch for release {release_id!r}")

    release.readiness_verified = True
    release.readiness_metadata = {
        **release.readiness_metadata,
        "artifact_bytes": artifact_bytes,
        "verified_at": datetime.now(UTC).isoformat(),
    }
    session.add(release)
    session.flush()
    _logger.info(
        "executor_release_lifecycle",
        extra={"event": "verified", "release_id": release.id, "status": release.status.value},
    )
    return release


def bootstrap_legacy_release(session: Session, release: ExecutorRelease) -> ExecutorRelease:
    """Pin existing benchmarks' initial admission metadata to a verified legacy release."""
    registered = register_release(session, release)
    active = promote_release(session, registered.id)

    benchmarks = session.exec(select(Benchmark).where(col(Benchmark.executor_release_id).is_(None))).all()
    for benchmark in benchmarks:
        pin_benchmark_to_release(benchmark, active)
        session.add(benchmark)
    session.flush()
    return active


def pin_benchmark_to_release(benchmark: Benchmark, release: ExecutorRelease) -> None:
    """Persist the initial executor identity for a benchmark exactly once."""
    if benchmark.executor_release_id is not None:
        raise ReleaseControlError(f"Benchmark {benchmark.id} already has executor release ownership")

    benchmark.executor_release_id = release.id
    benchmark.executor_artifact_uri = release.artifact_uri
    benchmark.executor_artifact_digest = release.artifact_digest
    benchmark.executor_protocol_version = release.protocol_version


def create_executor_dispatch(
    benchmark_id: UUID,
    release: ExecutorRelease,
    kind: ExecutorDispatchKind,
) -> ExecutorDispatch:
    """Snapshot the selected release for one queued executor invocation."""
    return ExecutorDispatch(
        benchmark_id=benchmark_id,
        kind=kind,
        executor_release_id=release.id,
        executor_artifact_uri=release.artifact_uri,
        executor_artifact_digest=release.artifact_digest,
        executor_protocol_version=release.protocol_version,
    )


def active_executor_release_counts(session: Session) -> dict[str, int]:
    """Count nonterminal dispatches plus pre-dispatch-ledger active benchmarks."""
    dispatch_rows = session.exec(
        select(ExecutorDispatch.executor_release_id, func.count())
        .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
        .group_by(col(ExecutorDispatch.executor_release_id))
    ).all()
    counts = {release_id: count for release_id, count in dispatch_rows}

    benchmarks_with_start_dispatch = select(ExecutorDispatch.benchmark_id).where(
        col(ExecutorDispatch.kind) == ExecutorDispatchKind.START
    )
    legacy_rows = session.exec(
        select(Benchmark.executor_release_id, func.count())
        .where(col(Benchmark.status).in_(_ACTIVE_BENCHMARK_STATUSES))
        .where(col(Benchmark.executor_release_id).is_not(None))
        .where(col(Benchmark.id).not_in(benchmarks_with_start_dispatch))
        .group_by(col(Benchmark.executor_release_id))
    ).all()
    for release_id, count in legacy_rows:
        assert release_id is not None
        counts[release_id] = counts.get(release_id, 0) + count
    return counts


def select_active_release(session: Session, *, for_update: bool = False) -> ExecutorRelease:
    """Return the healthy release currently admitted for new benchmarks."""
    admission = _get_admission(session, for_update=for_update)
    if admission is None or admission.release_id is None:
        raise ReleaseControlError("No active executor release is configured")

    release = session.get(ExecutorRelease, admission.release_id)
    if release is None or release.status != ExecutorReleaseStatus.ACTIVE or not release.readiness_verified:
        raise ReleaseControlError("No active executor release is configured")
    return release


def promote_release(session: Session, release_id: str) -> ExecutorRelease:
    """Promote a ready candidate or previously draining release for new benchmarks."""
    admission = _get_admission(session, for_update=True)
    release = _get_release(session, release_id, populate_existing=True, for_update=True)
    if release.status == ExecutorReleaseStatus.RETIRED:
        raise ReleaseControlError(f"Retired executor release {release_id!r} cannot be promoted")
    rolling_back = release.status == ExecutorReleaseStatus.DRAINING
    if not release.readiness_verified:
        raise ReleaseControlError(f"Executor release {release_id!r} is not ready")

    if admission is not None and admission.release_id == release_id:
        if release.status != ExecutorReleaseStatus.ACTIVE:
            raise ReleaseControlError("Admission points to a release that is not active")
        return release

    if admission is not None and admission.release_id is not None:
        previous = _get_release(session, admission.release_id, populate_existing=True)
        if previous.status == ExecutorReleaseStatus.ACTIVE:
            previous.status = ExecutorReleaseStatus.DRAINING
            previous.draining_at = previous.draining_at or datetime.now(UTC)
            session.add(previous)
            _logger.info(
                "executor_release_lifecycle",
                extra={"event": "draining", "release_id": previous.id, "status": previous.status.value},
            )

    release.status = ExecutorReleaseStatus.ACTIVE
    release.activated_at = release.activated_at or datetime.now(UTC)
    release.draining_at = None
    release.retired_at = None
    session.add(release)

    if admission is None:
        admission = ExecutorAdmission(release_id=release_id)
    else:
        admission.release_id = release_id
        admission.updated_at = datetime.now(UTC)
    session.add(admission)
    session.flush()
    _logger.info(
        "executor_release_lifecycle",
        extra={
            "event": "rollback" if rolling_back else "promoted",
            "release_id": release.id,
            "status": release.status.value,
        },
    )
    return release


def mark_draining(session: Session, release_id: str) -> ExecutorRelease:
    """Mark an admitted predecessor as draining without changing its timestamp."""
    admission = _get_admission(session, for_update=True)
    release = _get_release(session, release_id, populate_existing=True, for_update=True)
    if release.status == ExecutorReleaseStatus.DRAINING:
        return release
    if release.status != ExecutorReleaseStatus.ACTIVE:
        raise ReleaseControlError(f"Executor release {release_id!r} cannot drain from {release.status}")
    if admission is not None and admission.release_id == release_id:
        raise ReleaseControlError("The active executor release must be replaced before draining")

    release.status = ExecutorReleaseStatus.DRAINING
    release.draining_at = datetime.now(UTC)
    session.add(release)
    session.flush()
    _logger.info(
        "executor_release_lifecycle",
        extra={"event": "draining", "release_id": release.id, "status": release.status.value},
    )
    return release


def retire_if_empty(session: Session, release_id: str) -> bool:
    """Retire a drained release when it owns no active executor work."""
    admission = _get_admission(session, for_update=True)
    release = _get_release(session, release_id, populate_existing=True, for_update=True)
    if release.status == ExecutorReleaseStatus.RETIRED:
        return False
    if release.status != ExecutorReleaseStatus.DRAINING:
        raise ReleaseControlError(f"Executor release {release_id!r} cannot retire from {release.status}")
    if admission is not None and admission.release_id == release_id:
        raise ReleaseControlError(f"Executor release {release_id!r} is the active admission target")

    if active_executor_release_counts(session).get(release_id, 0):
        raise ReleaseControlError(f"Executor release {release_id!r} still has active executor work")

    retired_at = datetime.now(UTC)
    release.status = ExecutorReleaseStatus.RETIRED
    release.retired_at = retired_at
    release.artifact_retention_until = retired_at + timedelta(days=_ARTIFACT_RETENTION_DAYS)
    session.add(release)
    session.flush()
    _logger.info(
        "executor_release_lifecycle",
        extra={
            "event": "retired",
            "release_id": release.id,
            "status": release.status.value,
            "artifact_retention_until": release.artifact_retention_until.isoformat(),
        },
    )
    return True


def artifact_deletion_allowed(session: Session, release_id: str, *, now: datetime | None = None) -> bool:
    """Return true only after retirement retention expires and no active run owns the release."""
    release = _get_release(session, release_id)
    if release.status != ExecutorReleaseStatus.RETIRED or release.artifact_retention_until is None:
        return False
    retention_until = release.artifact_retention_until
    if retention_until.tzinfo is None:
        retention_until = retention_until.replace(tzinfo=UTC)
    if (now or datetime.now(UTC)) < retention_until:
        return False
    return active_executor_release_counts(session).get(release_id, 0) == 0


def _get_admission(session: Session, *, for_update: bool = False) -> ExecutorAdmission | None:
    statement = (
        select(ExecutorAdmission).where(col(ExecutorAdmission.id) == 1).execution_options(populate_existing=True)
    )
    if for_update:
        statement = statement.with_for_update()
    return session.exec(statement).one_or_none()


def _validate_release_manifest(release: ExecutorRelease) -> None:
    digest = release.artifact_digest.lower()
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ReleaseControlError("Executor artifact digest must be a 64-character SHA-256 digest")
    if release.protocol_version != _SUPPORTED_PROTOCOL_VERSION:
        raise ReleaseControlError(f"Unsupported executor protocol version: {release.protocol_version}")
    if not release.artifact_uri.startswith("s3://"):
        raise ReleaseControlError("Executor artifact URI must use s3://")
    release.artifact_digest = digest


def _get_release(
    session: Session,
    release_id: str,
    *,
    populate_existing: bool = False,
    for_update: bool = False,
) -> ExecutorRelease:
    if populate_existing or for_update:
        statement = (
            select(ExecutorRelease)
            .where(col(ExecutorRelease.id) == release_id)
            .execution_options(populate_existing=True)
        )
        if for_update:
            statement = statement.with_for_update()
        release = session.exec(statement).one_or_none()
    else:
        release = session.get(ExecutorRelease, release_id)
    if release is None:
        raise ReleaseControlError(f"Executor release {release_id!r} does not exist")
    return release
