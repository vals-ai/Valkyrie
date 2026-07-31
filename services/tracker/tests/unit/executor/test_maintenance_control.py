"""Contracts for the stage-wide executor maintenance fence."""

from uuid import UUID, uuid4

import pytest
from sqlmodel import Session, select

from executor_protocol import ExecutorDispatchStatus
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorRelease,
    ExecutorReleaseStatus,
    Org,
    Task,
    TaskStatus,
)
from tracker.executor.maintenance_control import MaintenanceOwnershipError, begin_maintenance, finish_maintenance
from tracker.executor.release_control import MaintenanceModeError, select_active_release


def _release(release_id: str = "maintenance-release") -> ExecutorRelease:
    return ExecutorRelease(
        id=release_id,
        artifact_uri=f"s3://artifacts/{release_id}.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        status=ExecutorReleaseStatus.ACTIVE,
        readiness_verified=True,
    )


def _benchmark(org_id: UUID, name: str) -> Benchmark:
    return Benchmark(
        id=uuid4(),
        org_id=org_id,
        name=name,
        status=BenchmarkStatus.IN_PROGRESS,
        arguments=BenchmarkArguments(
            contract=AgentContractRequest(name=f"{name}-agent", install_cmd="true", run_cmd="true"),
            concurrency=1,
        ),
        current_execution_release_id="maintenance-release",
    )


def test_begin_maintenance_globally_stops_work_and_closes_admission(database_session: Session) -> None:
    release = _release()
    database_session.add(release)
    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    admission.release_id = release.id
    database_session.add(admission)

    second_org = Org(id=uuid4(), name="maintenance-second-org")
    database_session.add(second_org)
    default_org = database_session.exec(select(Org).where(Org.name == "default")).one()
    benchmarks = [
        _benchmark(default_org.id, "first"),
        _benchmark(second_org.id, "second"),
    ]
    for benchmark in benchmarks:
        database_session.add(benchmark)
        database_session.add(
            Task(
                org_id=benchmark.org_id,
                benchmark=benchmark.id,
                task_id=f"{benchmark.name}-task",
                status=TaskStatus.IN_PROGRESS,
            )
        )
        database_session.add(
            ExecutorDispatch(
                benchmark_id=benchmark.id,
                kind=ExecutorDispatchKind.START,
                status=ExecutorDispatchStatus.RUNNING,
                executor_release_id=release.id,
                executor_artifact_uri=release.artifact_uri,
                executor_artifact_digest=release.artifact_digest,
                executor_protocol_version=release.protocol_version,
            )
        )
    database_session.commit()

    summary = begin_maintenance(database_session, target_sha="a" * 40)
    database_session.commit()

    assert summary.benchmarks == 2
    assert summary.tasks == 2
    assert summary.dispatches == 2
    assert all(benchmark.status == BenchmarkStatus.STOPPED for benchmark in benchmarks)
    assert all(task.status == TaskStatus.STOPPED for task in database_session.exec(select(Task)).all())
    assert all(
        dispatch.status == ExecutorDispatchStatus.FAILED
        for dispatch in database_session.exec(select(ExecutorDispatch)).all()
    )
    with pytest.raises(MaintenanceModeError, match="maintenance is in progress"):
        select_active_release(database_session, for_update=True)


def test_finish_maintenance_requires_current_target(database_session: Session) -> None:
    begin_maintenance(database_session, target_sha="a" * 40)
    database_session.commit()

    with pytest.raises(MaintenanceOwnershipError, match="does not own"):
        finish_maintenance(database_session, target_sha="b" * 40)
    database_session.rollback()

    finish_maintenance(database_session, target_sha="a" * 40)
    database_session.commit()

    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    assert admission.maintenance_target_sha is None


def test_begin_maintenance_is_idempotent_for_same_target(database_session: Session) -> None:
    first = begin_maintenance(database_session, target_sha="a" * 40)
    database_session.commit()
    second = begin_maintenance(database_session, target_sha="a" * 40)

    assert first == second


def test_begin_maintenance_rejects_a_different_target(database_session: Session) -> None:
    begin_maintenance(database_session, target_sha="a" * 40)
    database_session.commit()

    with pytest.raises(MaintenanceOwnershipError, match="another deployment"):
        begin_maintenance(database_session, target_sha="b" * 40)
    database_session.rollback()

    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    assert admission.maintenance_target_sha == "a" * 40
