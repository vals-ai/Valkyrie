"""Executor release, admission, and dispatch persistence operations."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import update
from sqlmodel import Session, col, select

from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    ExecutorAdmission,
    ExecutorRelease,
    ExecutorReleaseStatus,
    Task,
    TaskStatus,
)
from tracker.exceptions import MaintenanceModeError, MaintenanceOwnershipError, ReleaseControlError

_ACTIVE_BENCHMARK_STATUSES = (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)
_ACTIVE_DISPATCH_STATUSES = (
    ExecutorDispatchStatus.QUEUED,
    ExecutorDispatchStatus.RUNNING,
)
_ACTIVE_TASK_STATUSES = (
    TaskStatus.PENDING,
    TaskStatus.BUILDING,
    TaskStatus.IN_PROGRESS,
    TaskStatus.EVALUATING,
)
_ARTIFACT_RETENTION_DAYS = 30


@dataclass(frozen=True)
class ActiveExecutorReleaseWork:
    """Active dispatch and benchmark ownership grouped by executor release."""

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


class EnqueueFailureResolution(str, Enum):
    """Outcome of resolving a failed executor-dispatch enqueue."""

    FAILED = "FAILED"
    DELIVERED = "DELIVERED"
    SUPERSEDED = "SUPERSEDED"


@dataclass(frozen=True)
class MaintenanceStopSummary:
    """Counts of workloads stopped by one maintenance transition."""

    benchmarks: int
    tasks: int
    dispatches: int


class ExecutorControlRepository:
    """Stage executor-dispatch persistence in a caller-owned transaction."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def register_release(self, release: ExecutorRelease) -> ExecutorRelease:
        """Stage a new immutable candidate release after caller validation."""
        if self._session.get(ExecutorRelease, release.id) is not None:
            raise ReleaseControlError(f"Executor release {release.id!r} already exists")
        if release.status != ExecutorReleaseStatus.CANDIDATE:
            raise ReleaseControlError("New executor releases must start as candidates")
        self._session.add(release)
        self._session.flush()
        return release

    def get_release(
        self,
        release_id: str,
        *,
        populate_existing: bool = False,
        for_update: bool = False,
    ) -> ExecutorRelease:
        """Load a release, optionally refreshing and locking its row."""
        if populate_existing or for_update:
            statement = (
                select(ExecutorRelease)
                .where(col(ExecutorRelease.id) == release_id)
                .execution_options(populate_existing=True)
            )
            if for_update:
                statement = statement.with_for_update()
            release = self._session.exec(statement).one_or_none()
        else:
            release = self._session.get(ExecutorRelease, release_id)
        if release is None:
            raise ReleaseControlError(f"Executor release {release_id!r} does not exist")
        return release

    def find_release(self, release_id: str) -> ExecutorRelease | None:
        """Return a release without raising when it is absent."""
        return self._session.get(ExecutorRelease, release_id)

    def get_executor_admission(self, *, for_update: bool = False) -> ExecutorAdmission:
        """Return the singleton executor admission row."""
        statement = (
            select(ExecutorAdmission).where(col(ExecutorAdmission.id) == 1).execution_options(populate_existing=True)
        )
        if for_update:
            statement = statement.with_for_update()
        admission = self._session.exec(statement).one_or_none()
        if admission is None:
            raise ReleaseControlError("Executor admission state is not initialized")
        return admission

    def lock_executor_admission(self) -> ExecutorAdmission:
        """Lock executor admission and reject new work during maintenance."""
        admission = self.get_executor_admission(for_update=True)
        if admission.maintenance_target_sha is not None:
            raise MaintenanceModeError("Executor maintenance is in progress")
        return admission

    def select_active_release(self, *, for_update: bool = False) -> ExecutorRelease:
        """Return the healthy release currently admitted for new benchmarks."""
        try:
            admission = self.lock_executor_admission() if for_update else self._open_executor_admission()
        except ReleaseControlError as error:
            if str(error) == "Executor admission state is not initialized":
                raise ReleaseControlError("No active executor release is configured") from error
            raise
        if admission.release_id is None:
            raise ReleaseControlError("No active executor release is configured")
        release = self.get_release(admission.release_id)
        if release.status != ExecutorReleaseStatus.ACTIVE or not release.readiness_verified:
            raise ReleaseControlError("No active executor release is configured")
        return release

    def resolve_current_execution_release(
        self,
        benchmark: Benchmark,
        *,
        for_update: bool = False,
    ) -> ExecutorRelease:
        """Resolve the verified active or draining release owned by a benchmark."""
        release_id = benchmark.current_execution_release_id
        if release_id is None:
            raise ReleaseControlError(f"Benchmark {benchmark.id} has no current executor release")
        release = self.get_release(release_id, populate_existing=for_update, for_update=for_update)
        if (
            release.status not in (ExecutorReleaseStatus.ACTIVE, ExecutorReleaseStatus.DRAINING)
            or not release.readiness_verified
        ):
            raise ReleaseControlError(f"Current executor release {release_id!r} is unavailable")
        return release

    def pin_benchmark_to_release(self, benchmark: Benchmark, release: ExecutorRelease) -> None:
        """Stage the initial executor identity for a benchmark exactly once."""
        if benchmark.executor_release_id is not None:
            raise ReleaseControlError(f"Benchmark {benchmark.id} already has executor release ownership")
        benchmark.executor_release_id = release.id
        benchmark.current_execution_release_id = release.id
        benchmark.executor_artifact_uri = release.artifact_uri
        benchmark.executor_artifact_digest = release.artifact_digest
        benchmark.executor_protocol_version = release.protocol_version
        self._session.add(benchmark)

    def stage_benchmark_recovery(self, benchmark: Benchmark, release: ExecutorRelease) -> None:
        """Stage release identity for a retry or resume admission."""
        benchmark.current_execution_release_id = release.id
        benchmark.finished_at = None
        self._session.add(benchmark)

    def stage_dispatch(self, dispatch: ExecutorDispatch) -> ExecutorDispatch:
        """Stage and flush a dispatch row so its identity is visible to the caller."""
        self._session.add(dispatch)
        self._session.flush()
        return dispatch

    def mark_release_verified(self, release_id: str, metadata: dict[str, Any]) -> ExecutorRelease:
        """Stage successful external artifact verification metadata."""
        release = self.get_release(release_id)
        if release.status == ExecutorReleaseStatus.RETIRED:
            raise ReleaseControlError(f"Retired executor release {release_id!r} cannot be verified")
        release.readiness_verified = True
        release.readiness_metadata = {**release.readiness_metadata, **metadata}
        self._session.add(release)
        self._session.flush()
        return release

    def _open_executor_admission(self, *, for_update: bool = False) -> ExecutorAdmission:
        admission = self.get_executor_admission(for_update=for_update)
        if admission.maintenance_target_sha is not None:
            raise MaintenanceModeError("Executor maintenance is in progress")
        return admission

    def promote_release(self, release_id: str) -> ExecutorRelease:
        """Promote a verified candidate for new benchmarks."""
        admission = self.get_executor_admission(for_update=True)
        release = self.get_release(release_id, populate_existing=True, for_update=True)
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
            previous = self.get_release(admission.release_id, populate_existing=True)
            if previous.status == ExecutorReleaseStatus.ACTIVE:
                previous.status = ExecutorReleaseStatus.DRAINING
                previous.draining_at = previous.draining_at or datetime.now(UTC)
                self._session.add(previous)
        release.status = ExecutorReleaseStatus.ACTIVE
        release.activated_at = release.activated_at or datetime.now(UTC)
        release.draining_at = None
        release.retired_at = None
        self._session.add(release)
        admission.release_id = release_id
        admission.updated_at = datetime.now(UTC)
        self._session.add(admission)
        self._session.flush()
        return release

    def active_executor_release_work(self) -> ActiveExecutorReleaseWork:
        """Return active dispatch and benchmark ownership grouped by release."""
        active_dispatches = self._session.exec(
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
        active_benchmarks = self._session.exec(
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

    def retire_drained_releases(self) -> list[str]:
        """Retire draining releases that own no active executor work."""
        admission = self.get_executor_admission(for_update=True)
        draining_releases = self._session.exec(
            select(ExecutorRelease)
            .where(ExecutorRelease.status == ExecutorReleaseStatus.DRAINING)
            .order_by(ExecutorRelease.id)
            .with_for_update()
        ).all()
        if admission.release_id in {release.id for release in draining_releases}:
            raise ReleaseControlError("The active admission target cannot be draining")
        active_work = self.active_executor_release_work()
        if active_work.unattributed_executions:
            return []
        retired_ids: list[str] = []
        for release in draining_releases:
            if active_work.counts_by_release.get(release.id, 0):
                continue
            retired_at = datetime.now(UTC)
            release.status = ExecutorReleaseStatus.RETIRED
            release.retired_at = retired_at
            release.artifact_retention_until = retired_at + timedelta(days=_ARTIFACT_RETENTION_DAYS)
            self._session.add(release)
            retired_ids.append(release.id)
        self._session.flush()
        return retired_ids

    def artifact_deletion_allowed(self, release_id: str, *, now: datetime | None = None) -> bool:
        """Return whether a retired release artifact is past retention and unowned."""
        release = self.get_release(release_id)
        if release.status != ExecutorReleaseStatus.RETIRED or release.artifact_retention_until is None:
            return False
        retention_until = release.artifact_retention_until
        if retention_until.tzinfo is None:
            retention_until = retention_until.replace(tzinfo=UTC)
        active_work = self.active_executor_release_work()
        return (
            (now or datetime.now(UTC)) >= retention_until
            and not active_work.unattributed_executions
            and active_work.counts_by_release.get(release.id, 0) == 0
        )

    def begin_maintenance(self, target_sha: str) -> MaintenanceStopSummary:
        """Claim maintenance and stop all active executor workloads atomically."""
        if not target_sha:
            raise ValueError("Maintenance target SHA is required")

        admission = self.get_executor_admission(for_update=True)
        if admission.maintenance_target_sha not in (None, target_sha):
            raise MaintenanceOwnershipError("Executor maintenance is owned by another deployment")
        admission.maintenance_target_sha = target_sha
        admission.updated_at = datetime.now(UTC)
        self._session.add(admission)

        benchmarks = self._session.exec(
            select(Benchmark)
            .where(col(Benchmark.status).in_(_ACTIVE_BENCHMARK_STATUSES))
            .order_by(col(Benchmark.id))
            .with_for_update()
        ).all()
        for benchmark in benchmarks:
            benchmark.status = BenchmarkStatus.STOPPED
            self._session.add(benchmark)

        tasks = list(
            self._session.exec(
                select(Task).where(col(Task.status).in_(_ACTIVE_TASK_STATUSES)).order_by(col(Task.id)).with_for_update()
            ).all()
        )
        for task in tasks:
            task.status = TaskStatus.STOPPED
            self._session.add(task)

        dispatches = list(
            self._session.exec(
                select(ExecutorDispatch)
                .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
                .order_by(col(ExecutorDispatch.id))
                .with_for_update()
            ).all()
        )
        finished_at = datetime.now(UTC)
        for dispatch in dispatches:
            dispatch.status = ExecutorDispatchStatus.FAILED
            dispatch.finished_at = finished_at
            self._session.add(dispatch)

        self._session.flush()
        return MaintenanceStopSummary(
            benchmarks=len(benchmarks),
            tasks=len(tasks),
            dispatches=len(dispatches),
        )

    def finish_maintenance(self, target_sha: str) -> None:
        """Release the maintenance fence owned by ``target_sha``."""
        admission = self.get_executor_admission(for_update=True)
        if admission.maintenance_target_sha is None:
            return
        if admission.maintenance_target_sha != target_sha:
            raise MaintenanceOwnershipError("Deployment does not own executor maintenance")
        admission.maintenance_target_sha = None
        admission.updated_at = datetime.now(UTC)
        self._session.add(admission)
        self._session.flush()

    def terminalize_active_dispatches(
        self,
        benchmark_id: UUID,
        *,
        except_dispatch_id: UUID | None = None,
    ) -> None:
        """Fail every queued or running dispatch except an optional selected owner."""
        dispatches = (
            update(ExecutorDispatch)
            .where(col(ExecutorDispatch.benchmark_id) == benchmark_id)
            .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
        )
        if except_dispatch_id is not None:
            dispatches = dispatches.where(col(ExecutorDispatch.id) != except_dispatch_id)
        self._session.exec(
            dispatches.values(
                status=ExecutorDispatchStatus.FAILED,
                finished_at=datetime.now(ZoneInfo("UTC")),
            )
        )

    def active_dispatch_exists(
        self,
        benchmark_id: UUID,
        *,
        except_dispatch_id: UUID | None = None,
    ) -> bool:
        """Return whether another queued or running dispatch exists."""
        dispatches = (
            select(ExecutorDispatch.id)
            .where(col(ExecutorDispatch.benchmark_id) == benchmark_id)
            .where(col(ExecutorDispatch.status).in_(_ACTIVE_DISPATCH_STATUSES))
        )
        if except_dispatch_id is not None:
            dispatches = dispatches.where(col(ExecutorDispatch.id) != except_dispatch_id)
        return self._session.exec(dispatches).first() is not None

    def record_dispatch_failure(
        self,
        *,
        benchmark: Benchmark,
        dispatch_id: UUID,
        task_ids: list[str],
        error_message: str,
    ) -> bool:
        """Record a dispatch failure without overwriting newer task attempts."""
        dispatch = self._session.exec(
            select(ExecutorDispatch)
            .where(ExecutorDispatch.id == dispatch_id)
            .where(ExecutorDispatch.benchmark_id == benchmark.id)
            .where(ExecutorDispatch.status == ExecutorDispatchStatus.RUNNING)
            .with_for_update()
        ).one_or_none()
        if dispatch is None:
            return False

        now = datetime.now(ZoneInfo("UTC"))
        sibling_active = self.active_dispatch_exists(benchmark.id, except_dispatch_id=dispatch_id)

        tasks = self._session.exec(
            select(Task)
            .where(col(Task.benchmark) == benchmark.id)
            .where(col(Task.org_id) == benchmark.org_id)
            .where(col(Task.task_id).in_(task_ids))
            .where(col(Task.started_at) <= dispatch.created_at)
            .where(
                col(Task.status).in_(
                    (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
                )
            )
        ).all()
        for task in tasks:
            self._session.add(ErrorResult(org_id=task.org_id, task=task.id, error_message=error_message))
            task.status = TaskStatus.ERROR
            task.finished_at = now
            self._session.add(task)
        if sibling_active:
            dispatch.status = ExecutorDispatchStatus.FAILED
            dispatch.finished_at = now
            self._session.add(dispatch)
        else:
            benchmark.status = BenchmarkStatus.ERROR
            benchmark.finished_at = now
            benchmark.error_message = error_message
            self._session.add(benchmark)
        return True

    def resolve_enqueue_failure(
        self,
        *,
        benchmark_id: UUID,
        dispatch_id: UUID,
        task_ids: list[str],
    ) -> EnqueueFailureResolution:
        """Resolve an unclaimed dispatch without overriding delivered work."""
        benchmark = self._session.exec(
            select(Benchmark)
            .where(Benchmark.id == benchmark_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        ).one()
        dispatch = self._session.exec(
            select(ExecutorDispatch)
            .where(ExecutorDispatch.id == dispatch_id)
            .where(ExecutorDispatch.benchmark_id == benchmark_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        ).one()

        if dispatch.status != ExecutorDispatchStatus.QUEUED:
            resolution = (
                EnqueueFailureResolution.DELIVERED
                if dispatch.started_at is not None or dispatch.status == ExecutorDispatchStatus.FINISHED
                else EnqueueFailureResolution.SUPERSEDED
            )
            self._session.rollback()
            return resolution

        now = datetime.now(ZoneInfo("UTC"))
        dispatch.status = ExecutorDispatchStatus.FAILED
        dispatch.finished_at = now
        self._session.add(dispatch)
        self._session.flush()

        if benchmark.status == BenchmarkStatus.IN_PROGRESS and not self.active_dispatch_exists(benchmark_id):
            benchmark.status = BenchmarkStatus.ERROR
            benchmark.finished_at = now
            benchmark.error_message = "Executor dispatch enqueue failed"
            self._session.add(benchmark)

        tasks = self._session.exec(
            select(Task)
            .where(col(Task.benchmark) == benchmark_id)
            .where(col(Task.org_id) == benchmark.org_id)
            .where(col(Task.task_id).in_(task_ids))
            .where(col(Task.started_at) <= dispatch.created_at)
            .where(
                col(Task.status).in_(
                    (TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING)
                )
            )
        ).all()
        for task in tasks:
            task.status = TaskStatus.ERROR
            task.finished_at = now
            self._session.add(task)
        return EnqueueFailureResolution.FAILED
