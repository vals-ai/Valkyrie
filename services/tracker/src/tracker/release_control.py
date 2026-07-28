"""Lifecycle operations for immutable executor releases in PostgreSQL."""

import hashlib
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast
from urllib.parse import urlparse
from uuid import UUID

import boto3
from sqlmodel import Session, col, select

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
from executor_protocol import (
    SUPPORTED_PROTOCOL_VERSION,
    validate_executor_artifact_uri,
    validate_executor_digest,
)

_ACTIVE_BENCHMARK_STATUSES = (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)
_ACTIVE_DISPATCH_STATUSES = (
    ExecutorDispatchStatus.QUEUED,
    ExecutorDispatchStatus.RUNNING,
)
_ARTIFACT_RETENTION_DAYS = 30
_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActiveExecutorReleaseWork:
    dispatches_by_release: dict[str, list[ExecutorDispatch]]
    executions_by_release: dict[str, list[Benchmark]]
    unattributed_executions: list[Benchmark]

    @property
    def counts_by_release(self) -> dict[str, int]:
        release_ids = self.dispatches_by_release.keys() | self.executions_by_release.keys()
        return {
            release_id: len(self.dispatches_by_release.get(release_id, []))
            + len(self.executions_by_release.get(release_id, []))
            for release_id in release_ids
        }


class S3Body(Protocol):
    def read(self, size: int = -1) -> bytes: ...

    def close(self) -> None: ...


class S3Client(Protocol):
    def get_object(self, *, Bucket: str, Key: str) -> Mapping[str, object]: ...


class ReleaseControlError(ValueError):
    """Raised when a release lifecycle transition violates ownership rules."""


class MaintenanceModeError(ReleaseControlError):
    """Raised when executor admission is closed for deployment maintenance."""


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


def activate_release(
    session: Session,
    candidate: ExecutorRelease,
    *,
    expected_bucket: str,
    expected_prefix: str,
    s3_client: S3Client | None = None,
) -> ExecutorRelease:
    """Create-or-match, verify, promote, and assert one immutable release."""
    _validate_release_manifest(candidate)
    try:
        validate_executor_artifact_uri(candidate.artifact_uri, expected_bucket, expected_prefix)
    except ValueError as error:
        raise ReleaseControlError(str(error)) from error

    _get_required_admission(session, for_update=True)
    release = session.get(ExecutorRelease, candidate.id)
    if release is None:
        release = register_release(session, candidate)
    elif (
        release.artifact_uri != candidate.artifact_uri
        or release.artifact_digest != candidate.artifact_digest
        or release.protocol_version != candidate.protocol_version
    ):
        raise ReleaseControlError(f"Executor release {candidate.id!r} already has a different immutable identity")
    if release.status in (ExecutorReleaseStatus.DRAINING, ExecutorReleaseStatus.RETIRED):
        raise ReleaseControlError(f"Executor release {release.id!r} cannot be activated from {release.status.value}")

    verify_release_artifact(session, release.id, s3_client=s3_client)
    activated = promote_release(session, release.id)
    admission = _get_required_admission(session)
    if (
        admission.release_id != activated.id
        or activated.status != ExecutorReleaseStatus.ACTIVE
        or not activated.readiness_verified
    ):
        raise ReleaseControlError(f"Executor release {activated.id!r} did not become active")
    return activated


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


def pin_benchmark_to_release(benchmark: Benchmark, release: ExecutorRelease) -> None:
    """Persist the initial executor identity for a benchmark exactly once."""
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
    """Snapshot one release identity for queued execution."""
    return ExecutorDispatch(
        id=dispatch_id,
        benchmark_id=benchmark_id,
        kind=kind,
        executor_release_id=release.id,
        executor_artifact_uri=release.artifact_uri,
        executor_artifact_digest=release.artifact_digest,
        executor_protocol_version=release.protocol_version,
    )


def active_executor_release_work(session: Session) -> ActiveExecutorReleaseWork:
    """Return active dispatch, current-owner, and unattributed retirement blockers."""
    active_dispatches = session.exec(
        select(ExecutorDispatch)
        .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
        .order_by(col(ExecutorDispatch.created_at), col(ExecutorDispatch.id))
    ).all()
    dispatches_by_release: dict[str, list[ExecutorDispatch]] = {}
    active_dispatch_owners: set[tuple[UUID, str]] = set()
    for dispatch in active_dispatches:
        dispatches_by_release.setdefault(dispatch.executor_release_id, []).append(dispatch)
        active_dispatch_owners.add((dispatch.benchmark_id, dispatch.executor_release_id))

    executions_by_release: dict[str, list[Benchmark]] = {}
    unattributed_executions: list[Benchmark] = []
    active_benchmarks = session.exec(
        select(Benchmark)
        .where(col(Benchmark.status).in_(_ACTIVE_BENCHMARK_STATUSES))
        .order_by(col(Benchmark.started_at), col(Benchmark.id))
    ).all()
    for benchmark in active_benchmarks:
        release_id = benchmark.current_execution_release_id
        if release_id is None:
            unattributed_executions.append(benchmark)
        elif (benchmark.id, release_id) not in active_dispatch_owners:
            executions_by_release.setdefault(release_id, []).append(benchmark)

    return ActiveExecutorReleaseWork(
        dispatches_by_release=dispatches_by_release,
        executions_by_release=executions_by_release,
        unattributed_executions=unattributed_executions,
    )


def get_executor_admission(session: Session, *, for_update: bool = False) -> ExecutorAdmission:
    """Return the singleton executor admission row."""
    return _get_required_admission(session, for_update=for_update)


def _get_open_executor_admission(session: Session, *, for_update: bool) -> ExecutorAdmission:
    admission = _get_admission(session, for_update=for_update)
    if admission is None:
        raise ReleaseControlError("No active executor release is configured")
    if admission.maintenance_target_sha is not None:
        raise MaintenanceModeError("Executor maintenance is in progress")
    return admission


def lock_executor_admission(session: Session) -> ExecutorAdmission:
    """Lock executor admission and reject new work during maintenance."""
    return _get_open_executor_admission(session, for_update=True)


def select_active_release(session: Session, *, for_update: bool = False) -> ExecutorRelease:
    """Return the healthy release currently admitted for new benchmarks."""
    admission = _get_open_executor_admission(session, for_update=for_update)
    if admission.release_id is None:
        raise ReleaseControlError("No active executor release is configured")

    release = session.get(ExecutorRelease, admission.release_id)
    if release is None or release.status != ExecutorReleaseStatus.ACTIVE or not release.readiness_verified:
        raise ReleaseControlError("No active executor release is configured")
    return release


def resolve_current_execution_release(
    session: Session,
    benchmark: Benchmark,
    *,
    for_update: bool = False,
) -> ExecutorRelease:
    """Resolve the verified ACTIVE or DRAINING release owned by an active benchmark."""
    release_id = benchmark.current_execution_release_id
    if release_id is None:
        raise ReleaseControlError(f"Benchmark {benchmark.id} has no current executor release")

    release = session.get(
        ExecutorRelease,
        release_id,
        populate_existing=for_update,
        with_for_update=for_update or None,
    )
    if (
        release is None
        or release.status not in (ExecutorReleaseStatus.ACTIVE, ExecutorReleaseStatus.DRAINING)
        or not release.readiness_verified
    ):
        raise ReleaseControlError(f"Current executor release {release_id!r} is unavailable")
    return release


def promote_release(session: Session, release_id: str) -> ExecutorRelease:
    """Promote a verified candidate for new benchmarks."""
    admission = _get_required_admission(session, for_update=True)
    release = _get_release(session, release_id, populate_existing=True, for_update=True)

    if admission.release_id == release_id:
        if release.status != ExecutorReleaseStatus.ACTIVE:
            raise ReleaseControlError("Admission points to a release that is not active")
        if not release.readiness_verified:
            raise ReleaseControlError(f"Executor release {release_id!r} is not ready")
        return release
    if release.status != ExecutorReleaseStatus.CANDIDATE:
        raise ReleaseControlError(f"Executor release {release_id!r} cannot be promoted from {release.status.value}")
    if not release.readiness_verified:
        raise ReleaseControlError(f"Executor release {release_id!r} is not ready")

    if admission.release_id is not None:
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

    admission.release_id = release_id
    admission.updated_at = datetime.now(UTC)
    session.add(admission)
    session.flush()
    _logger.info(
        "executor_release_lifecycle",
        extra={"event": "promoted", "release_id": release.id, "status": release.status.value},
    )
    return release


def retire_drained_releases(session: Session) -> list[str]:
    """Retire every draining release that owns no active executor work."""
    admission = _get_required_admission(session, for_update=True)
    draining_releases = session.exec(
        select(ExecutorRelease)
        .where(ExecutorRelease.status == ExecutorReleaseStatus.DRAINING)
        .order_by(ExecutorRelease.id)
        .with_for_update()
    ).all()
    if admission.release_id in {release.id for release in draining_releases}:
        raise ReleaseControlError("The active admission target cannot be draining")

    active_work = active_executor_release_work(session)
    if active_work.unattributed_executions:
        return []

    retired_ids: list[str] = []
    for release in draining_releases:
        if active_work.counts_by_release.get(release.id, 0):
            continue
        _retire_release(session, release)
        retired_ids.append(release.id)
    session.flush()
    return retired_ids


def _retire_release(session: Session, release: ExecutorRelease) -> None:
    retired_at = datetime.now(UTC)
    release.status = ExecutorReleaseStatus.RETIRED
    release.retired_at = retired_at
    release.artifact_retention_until = retired_at + timedelta(days=_ARTIFACT_RETENTION_DAYS)
    session.add(release)
    _logger.info(
        "executor_release_lifecycle",
        extra={
            "event": "retired",
            "release_id": release.id,
            "status": release.status.value,
            "artifact_retention_until": release.artifact_retention_until.isoformat(),
        },
    )


def _artifact_deletion_allowed(
    release: ExecutorRelease,
    active_work: ActiveExecutorReleaseWork,
    now: datetime,
) -> bool:
    if release.status != ExecutorReleaseStatus.RETIRED or release.artifact_retention_until is None:
        return False
    retention_until = release.artifact_retention_until
    if retention_until.tzinfo is None:
        retention_until = retention_until.replace(tzinfo=UTC)
    return (
        now >= retention_until
        and not active_work.unattributed_executions
        and active_work.counts_by_release.get(release.id, 0) == 0
    )


def artifact_deletion_allowed(session: Session, release_id: str, *, now: datetime | None = None) -> bool:
    """Return true only after retirement retention expires and no active run owns the release."""
    release = _get_release(session, release_id)
    if release.status != ExecutorReleaseStatus.RETIRED or release.artifact_retention_until is None:
        return False
    return _artifact_deletion_allowed(
        release,
        active_executor_release_work(session),
        now or datetime.now(UTC),
    )


def _get_admission(session: Session, *, for_update: bool = False) -> ExecutorAdmission | None:
    statement = (
        select(ExecutorAdmission).where(col(ExecutorAdmission.id) == 1).execution_options(populate_existing=True)
    )
    if for_update:
        statement = statement.with_for_update()
    return session.exec(statement).one_or_none()


def _get_required_admission(session: Session, *, for_update: bool = False) -> ExecutorAdmission:
    admission = _get_admission(session, for_update=for_update)
    if admission is None:
        raise ReleaseControlError("Executor admission state is not initialized")
    return admission


def _validate_release_manifest(release: ExecutorRelease) -> None:
    try:
        release.artifact_digest = validate_executor_digest(release.artifact_digest)
    except ValueError as error:
        raise ReleaseControlError(str(error)) from error
    if release.protocol_version != SUPPORTED_PROTOCOL_VERSION:
        raise ReleaseControlError(f"Unsupported executor protocol version: {release.protocol_version}")
    if not release.artifact_uri.startswith("s3://"):
        raise ReleaseControlError("Executor artifact URI must use s3://")


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
