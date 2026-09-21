"""Lifecycle ownership and execution fencing on real PostgreSQL."""

import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from typing import Any
from uuid import uuid4

import pytest
import services.executor_host.supervisor as host_supervisor
from services.executor_host.supervisor import (
    ArtifactDispatch,
    DispatchAuthority,
    DispatchAuthorityLostError,
    ExecutorProcessPayload,
    ExecutorSupervisor,
    PostgresExecutorDispatchStore,
    run_executor_dispatch,
)
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tracker.aws.runtime import AWSResources
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Org,
    RunLifecycle,
    TaskStatus,
)
from tracker.exceptions import ExecutionAuthorityRevoked
from tracker.executor.dispatch_control import admit_recovery_dispatch, reconcile_expired_dispatches
from tracker.executor.execution_authority import ExecutionAuthority, lock_execution_authority
from tracker.executor.release_control import create_executor_dispatch, promote_release, register_release
from tracker.lifecycle import (
    LifecycleConflict,
    OperationIdentity,
    Purpose,
    RunScope,
    acquire_hold,
    release_relocation_hold,
)
from tracker.lifecycle_evidence import ExternalHostDrain, HostContractObservation, verify_drain
from tracker.utils.resources import update_benchmark_concurrency, update_benchmark_resume_arguments
from tracker.utils.run_control import apply_stop_benchmark


def seeded_run(
    postgres_session: Session,
) -> tuple[OperationIdentity, RunScope, Benchmark, ExecutorDispatch, ExecutorRelease]:

    org = Org(id=uuid4(), name=str(uuid4()))
    run = make_benchmark(org_id=org.id)
    resources = AWSResources(region="us-west-2", s3_bucket="owner-data", log_group="runs", log_retention_days=7)
    run.arguments = run.arguments.model_copy(update={"properties": resources})
    release = ExecutorRelease(
        id=str(uuid4()),
        artifact_uri="s3://releases/a",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    promote_release(postgres_session, release.id)
    postgres_session.add(run)
    postgres_session.flush()
    dispatch = create_executor_dispatch(run.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    dispatch.status = ExecutorDispatchStatus.RUNNING
    dispatch.started_at = datetime.now(UTC)
    postgres_session.add(dispatch)
    postgres_session.commit()
    identity = OperationIdentity(
        operation_id=uuid4(),
        parent_plan_sha256="a" * 64,
        github_owner_id=42,
        org_id=org.id,
        source_aws_account_id="123456789012",
        destination_aws_account_id="123456789012",
        region="us-west-2",
        environment="test",
        database_target="tracker-test",
        run_ids=(run.id,),
    )
    scope = RunScope(run_id=run.id, original_resources=resources)
    return identity, scope, run, dispatch, release


def test_hold_blocks_recovery_terminal_authority_and_wrong_resume(postgres_session: Session) -> None:
    identity, scope, run, dispatch, _ = seeded_run(postgres_session)
    acquire_hold(postgres_session, identity=identity, scope=scope, purpose="deletion")
    postgres_session.commit()
    with pytest.raises(LifecycleConflict):
        admit_recovery_dispatch(
            postgres_session,
            benchmark=run,
            pre_action_status=run.status,
            dispatch_id=uuid4(),
            kind=ExecutorDispatchKind.RETRY,
        )
    postgres_session.rollback()
    with pytest.raises(ExecutionAuthorityRevoked):
        lock_execution_authority(postgres_session, ExecutionAuthority(run.id, dispatch.id), require_in_progress=False)
    postgres_session.rollback()
    with pytest.raises(LifecycleConflict):
        acquire_hold(
            postgres_session,
            identity=identity.model_copy(update={"operation_id": uuid4()}),
            scope=scope,
            purpose="deletion",
        )


def test_permanent_hold_survives_run_removal(postgres_session: Session) -> None:
    identity, scope, run, dispatch, _ = seeded_run(postgres_session)
    acquire_hold(postgres_session, identity=identity, scope=scope, purpose="deletion")
    postgres_session.delete(dispatch)
    postgres_session.delete(run)
    postgres_session.commit()
    record = acquire_hold(postgres_session, identity=identity, scope=scope, purpose="deletion")
    assert record.released_at is None

    def complete(_session: Session, _record: RunLifecycle) -> None:
        pass

    with pytest.raises(LifecycleConflict):
        release_relocation_hold(postgres_session, identity=identity, scope=scope, verify_completion=complete)


def test_relocation_release_is_explicit_and_exact(postgres_session: Session) -> None:
    identity, scope, _, _, _ = seeded_run(postgres_session)
    acquire_hold(postgres_session, identity=identity, scope=scope, purpose="relocation")
    postgres_session.commit()

    def incomplete(_session: Session, _record: RunLifecycle) -> None:
        raise LifecycleConflict("Source cleanup remains pending")

    with pytest.raises(LifecycleConflict):
        release_relocation_hold(postgres_session, identity=identity, scope=scope, verify_completion=incomplete)
    postgres_session.rollback()
    assert acquire_hold(postgres_session, identity=identity, scope=scope, purpose="relocation").released_at is None
    verified: list[str] = []

    def complete(_session: Session, record: RunLifecycle) -> None:
        verified.append(record.phase)

    release_relocation_hold(postgres_session, identity=identity, scope=scope, verify_completion=complete)
    postgres_session.commit()
    assert verified == ["held"]
    with pytest.raises(LifecycleConflict):
        acquire_hold(postgres_session, identity=identity, scope=scope, purpose="relocation")
    postgres_session.rollback()
    replacement = identity.model_copy(update={"operation_id": uuid4()})
    with pytest.raises(LifecycleConflict):
        acquire_hold(postgres_session, identity=replacement, scope=scope, purpose="relocation")
    postgres_session.rollback()
    record = acquire_hold(
        postgres_session,
        identity=replacement,
        scope=scope,
        purpose="deletion",
        replace_released_operation_id=identity.operation_id,
    )
    assert record.purpose == "deletion"


@pytest.mark.asyncio
async def test_host_hold_claim_heartbeat_exit_and_single_use(
    postgres_session: Session, postgres_engine: Engine
) -> None:

    identity, scope, run, dispatch, release = seeded_run(postgres_session)
    url = postgres_engine.url
    store = PostgresExecutorDispatchStore(
        host=str(url.host),
        port=str(url.port),
        dbname=str(url.database),
        user=str(url.username),
        password=str(url.password),
    )
    artifact = ArtifactDispatch.from_payload(
        {
            "executor_release_id": release.id,
            "executor_artifact_uri": release.artifact_uri,
            "executor_artifact_digest": release.artifact_digest,
            "executor_protocol_version": release.protocol_version,
        }
    )
    dispatch.status = ExecutorDispatchStatus.QUEUED
    dispatch.started_at = None
    postgres_session.add(dispatch)
    postgres_session.commit()
    authority = await store.claim(str(dispatch.id), str(run.id), artifact)
    assert authority is not None
    assert await store.claim(str(dispatch.id), str(run.id), artifact) is None
    acquire_hold(postgres_session, identity=identity, scope=scope, purpose="deletion")
    postgres_session.commit()
    assert not await store.is_current(authority)
    assert not await store.heartbeat(authority)
    assert not await store.finish(authority)
    dispatch.status = ExecutorDispatchStatus.FAILED
    postgres_session.add(dispatch)
    postgres_session.commit()
    assert not await store.is_current(authority)
    assert not await store.heartbeat(authority)
    await store.acknowledge_exit(DispatchAuthority(str(dispatch.id), str(uuid4())))
    postgres_session.refresh(dispatch)
    assert dispatch.process_exited_at is None
    await store.acknowledge_exit(authority)
    postgres_session.refresh(dispatch)
    first_exit = dispatch.process_exited_at
    assert first_exit is not None
    await store.acknowledge_exit(authority)
    assert await store.claim(str(dispatch.id), str(run.id), artifact) is None
    postgres_session.refresh(dispatch)
    assert dispatch.process_exited_at == first_exit
    assert dispatch.status == ExecutorDispatchStatus.FAILED


@pytest.mark.asyncio
async def test_exit_receipt_is_utc_under_a_local_database_time_zone(
    postgres_session: Session, postgres_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:

    _, _, run, dispatch, release = seeded_run(postgres_session)
    url = postgres_engine.url
    monkeypatch.setenv("PGTZ", "America/Los_Angeles")
    store = PostgresExecutorDispatchStore(
        host=str(url.host),
        port=str(url.port),
        dbname=str(url.database),
        user=str(url.username),
        password=str(url.password),
    )
    artifact = ArtifactDispatch.from_payload(
        {
            "executor_release_id": release.id,
            "executor_artifact_uri": release.artifact_uri,
            "executor_artifact_digest": release.artifact_digest,
            "executor_protocol_version": release.protocol_version,
        }
    )
    dispatch.status = ExecutorDispatchStatus.QUEUED
    dispatch.started_at = None
    postgres_session.add(dispatch)
    postgres_session.commit()
    authority = await store.claim(str(dispatch.id), str(run.id), artifact)
    assert authority is not None

    await store.acknowledge_exit(authority)
    postgres_session.refresh(dispatch)
    exit_time = dispatch.process_exited_at
    assert exit_time is not None
    assert abs(exit_time.replace(tzinfo=UTC) - datetime.now(UTC)) < timedelta(minutes=5)


@pytest.mark.parametrize("action", ["retry", "claim"])
def test_hold_serializes_with_execution(postgres_session: Session, postgres_engine: Engine, action: str) -> None:
    identity, scope, run, dispatch, release = seeded_run(postgres_session)
    dispatch.status = ExecutorDispatchStatus.QUEUED
    dispatch.started_at = None
    postgres_session.add(dispatch)
    postgres_session.commit()
    run_id, dispatch_id, status = run.id, dispatch.id, run.status
    artifact = ArtifactDispatch.from_payload(
        {
            "executor_release_id": release.id,
            "executor_artifact_uri": release.artifact_uri,
            "executor_artifact_digest": release.artifact_digest,
            "executor_protocol_version": release.protocol_version,
        }
    )
    url = postgres_engine.url
    store = PostgresExecutorDispatchStore(
        host=str(url.host),
        port=str(url.port),
        dbname=str(url.database),
        user=str(url.username),
        password=str(url.password),
    )
    acquire_hold(postgres_session, identity=identity, scope=scope, purpose="deletion")

    def execute() -> str:
        if action == "claim":
            assert asyncio.run(store.claim(str(dispatch_id), str(run_id), artifact)) is None
            return "rejected"
        with Session(postgres_engine) as session:
            benchmark = session.get(Benchmark, run_id)
            assert benchmark is not None
            with pytest.raises(LifecycleConflict):
                admit_recovery_dispatch(
                    session,
                    benchmark=benchmark,
                    pre_action_status=status,
                    dispatch_id=uuid4(),
                    kind=ExecutorDispatchKind.RETRY,
                )
            session.rollback()
        return "rejected"

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(execute)
        try:
            with postgres_engine.connect() as observer:
                deadline = monotonic() + 5
                while monotonic() < deadline:
                    waiting = observer.execute(
                        text("""SELECT count(*) FROM pg_stat_activity
                        WHERE datname = current_database() AND wait_event_type = 'Lock'
                        AND pid != pg_backend_pid()""")
                    ).scalar_one()
                    if waiting:
                        break
                    assert not future.done()
                    sleep(0.01)
                else:
                    pytest.fail("Execution did not wait for the run lock")
        finally:
            postgres_session.commit()
        assert future.result(timeout=5) == "rejected"


@pytest.mark.parametrize("mismatch", [None, "operation", "digest", "dispatch", "host", "current_host", "future_cutoff"])
def test_legacy_external_drain_is_exact_and_distinct(postgres_session: Session, mismatch: str | None) -> None:
    identity, scope, _, dispatch, _ = seeded_run(postgres_session)
    dispatch.status = ExecutorDispatchStatus.FAILED
    dispatch.started_at = datetime(2025, 1, 1)
    dispatch.finished_at = datetime.now(UTC)
    postgres_session.add(dispatch)
    record = acquire_hold(postgres_session, identity=identity, scope=scope, purpose="deletion")
    postgres_session.commit()
    contract = HostContractObservation(
        contract="stable-host-lifecycle-v1",
        deployment_sha256="b" * 64,
        host_inventory=("host-1",),
        observed_at=datetime.now(UTC),
        acknowledgement_required_since=datetime(2026, 1, 1, tzinfo=UTC),
        verifier="operator",
        legacy_dispatch_ids=(dispatch.id,),
    )
    assert (
        verify_drain(postgres_session, identity=identity, scope=scope, purpose="deletion", host_contract=contract)[
            0
        ].provenance
        == "pending"
    )
    evidence = b"operator receipt for the complete terminated host inventory"
    acquired_at = record.acquired_at.replace(tzinfo=UTC)
    external = ExternalHostDrain(
        provenance="externally_confirmed_host_drain",
        identity=identity,
        run_id=scope.run_id,
        hold_acquired_at=acquired_at,
        dispatch_ids=(dispatch.id,),
        host_inventory=("host-1",),
        deployed_host_contract="stable-host-lifecycle-v1",
        observed_at=datetime.now(UTC),
        verifier="named-operator",
        evidence_sha256=hashlib.sha256(evidence).hexdigest(),
        confirmation="all_inventory_hosts_terminated_and_old_claims_disabled",
    )
    if mismatch == "operation":
        external = external.model_copy(update={"identity": identity.model_copy(update={"operation_id": uuid4()})})
    elif mismatch == "digest":
        evidence = b"other receipt"
    elif mismatch == "dispatch":
        external = external.model_copy(update={"dispatch_ids": (uuid4(),)})
    elif mismatch == "host":
        external = external.model_copy(update={"host_inventory": ("other-host",)})
    elif mismatch == "future_cutoff":
        contract = contract.model_copy(update={"acknowledgement_required_since": datetime.now(UTC) + timedelta(days=1)})
    elif mismatch == "current_host":
        dispatch.started_at = datetime.now(UTC)
        postgres_session.add(dispatch)
        postgres_session.flush()
    if mismatch is not None:
        with pytest.raises(LifecycleConflict):
            verify_drain(
                postgres_session,
                identity=identity,
                scope=scope,
                purpose="deletion",
                host_contract=contract,
                external=external,
                external_evidence=evidence,
            )
    else:
        results = verify_drain(
            postgres_session,
            identity=identity,
            scope=scope,
            purpose="deletion",
            host_contract=contract,
            external=external,
            external_evidence=evidence,
        )
        assert results[0].provenance == "externally_confirmed_host_drain"
        assert results[0].observed_exit_at is None


def test_verified_contract_only_accepts_finished_or_unclaimed(postgres_session: Session) -> None:
    identity, scope, _, dispatch, _ = seeded_run(postgres_session)
    acquire_hold(postgres_session, identity=identity, scope=scope, purpose="deletion")
    contract = HostContractObservation(
        contract="stable-host-lifecycle-v1",
        deployment_sha256="b" * 64,
        host_inventory=("host-1",),
        observed_at=datetime.now(UTC),
        acknowledgement_required_since=datetime(2026, 1, 1, tzinfo=UTC),
        verifier="operator",
    )
    for status, started, provenance in [
        (ExecutorDispatchStatus.FAILED, datetime.now(UTC), "pending"),
        (ExecutorDispatchStatus.FINISHED, datetime.now(UTC), "verified_finished_contract"),
        (ExecutorDispatchStatus.QUEUED, None, "held_unclaimed"),
    ]:
        dispatch.status, dispatch.started_at = status, started
        postgres_session.add(dispatch)
        postgres_session.flush()
        assert (
            verify_drain(postgres_session, identity=identity, scope=scope, purpose="deletion", host_contract=contract)[
                0
            ].provenance
            == provenance
        )


def test_hold_blocks_resume_changes_but_allows_force_stop(postgres_session: Session) -> None:

    identity, scope, run, dispatch, _ = seeded_run(postgres_session)
    org = postgres_session.get(Org, identity.org_id)
    assert org is not None
    acquire_hold(postgres_session, identity=identity, scope=scope, purpose="deletion")
    postgres_session.commit()
    with pytest.raises(LifecycleConflict):
        update_benchmark_concurrency(run.id, 10, postgres_session, org)
    postgres_session.rollback()
    with pytest.raises(LifecycleConflict):
        update_benchmark_resume_arguments(run.id, postgres_session, org, secrets={}, concurrency=10, benchmark_url=None)
    postgres_session.rollback()
    apply_stop_benchmark(run, postgres_session, force=True, org=org)
    postgres_session.refresh(dispatch)
    assert dispatch.status == ExecutorDispatchStatus.FAILED
    assert dispatch.process_exited_at is None


def test_lease_recovery_does_not_change_held_run(postgres_session: Session) -> None:
    identity, scope, run, dispatch, _ = seeded_run(postgres_session)
    dispatch.lease_expires_at = datetime.now(UTC) - timedelta(hours=1)
    postgres_session.add(dispatch)
    acquire_hold(postgres_session, identity=identity, scope=scope, purpose="deletion")
    postgres_session.commit()
    original_status = run.status
    assert reconcile_expired_dispatches(postgres_session) == 0
    postgres_session.refresh(run)
    assert run.status == original_status


@pytest.mark.parametrize("new_purpose", ["deletion", "relocation"])
def test_stale_released_hold_cannot_replace_new_owner(
    postgres_session: Session, postgres_engine: Engine, new_purpose: Purpose
) -> None:
    original_identity, scope, _, _, _ = seeded_run(postgres_session)
    cached_record = acquire_hold(postgres_session, identity=original_identity, scope=scope, purpose="relocation")

    def verify_completion(_session: Session, record: RunLifecycle) -> None:
        assert record.phase == "held"

    release_relocation_hold(
        postgres_session, identity=original_identity, scope=scope, verify_completion=verify_completion
    )
    postgres_session.commit()
    assert cached_record.released_at is not None

    new_identity = original_identity.model_copy(update={"operation_id": uuid4()})
    with Session(postgres_engine) as replacing_session:
        new_record = acquire_hold(
            replacing_session,
            identity=new_identity,
            scope=scope,
            purpose=new_purpose,
            replace_released_operation_id=original_identity.operation_id,
        )
        replacing_session.commit()
        replacing_session.refresh(new_record)
        saved_record = new_record.model_dump()

    # Keep the old ORM instance alive to reproduce identity-map reuse after locking.
    assert cached_record.purpose == "relocation"
    assert cached_record.released_at is not None
    assert OperationIdentity.model_validate_json(cached_record.identity_json) == original_identity

    attempted_identity = original_identity.model_copy(update={"operation_id": uuid4()})
    with pytest.raises(LifecycleConflict, match="Another lifecycle operation owns this run"):
        acquire_hold(
            postgres_session,
            identity=attempted_identity,
            scope=scope,
            purpose="relocation",
            replace_released_operation_id=original_identity.operation_id,
        )
    postgres_session.commit()

    with Session(postgres_engine) as verification_session:
        persisted_record = verification_session.get(RunLifecycle, scope.run_id)
        assert persisted_record is not None
        assert persisted_record.model_dump() == saved_record
        assert persisted_record.purpose == new_purpose
        assert persisted_record.released_at is None
        assert OperationIdentity.model_validate_json(persisted_record.identity_json) == new_identity


@pytest.mark.asyncio
@pytest.mark.parametrize("hold_after_poll", [True, False])
async def test_host_failure_respects_hold_after_authority_poll(
    postgres_session: Session,
    postgres_engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hold_after_poll: bool,
) -> None:
    identity, scope, run, dispatch, _ = seeded_run(postgres_session)
    marker = tmp_path / "child-started"
    script = (
        f"from pathlib import Path\nimport time\nPath({str(marker)!r}).touch()\n"
        + ("time.sleep(60)\n" if hold_after_poll else "raise SystemExit(1)\n")
    ).encode()
    digest = hashlib.sha256(script).hexdigest()
    (tmp_path / f"{digest}.pex").write_bytes(script)
    dispatch.status = ExecutorDispatchStatus.QUEUED
    dispatch.started_at = None
    dispatch.executor_artifact_uri = "s3://releases/executors/test.pex"
    dispatch.executor_artifact_digest = digest
    task = make_task(run, "task", status=TaskStatus.IN_PROGRESS)
    task.started_at = dispatch.created_at - timedelta(seconds=1)
    postgres_session.add_all([dispatch, task])
    postgres_session.commit()
    artifact = ArtifactDispatch.from_payload(
        {
            "executor_release_id": dispatch.executor_release_id,
            "executor_artifact_uri": dispatch.executor_artifact_uri,
            "executor_artifact_digest": digest,
            "executor_protocol_version": dispatch.executor_protocol_version,
        }
    )
    url = postgres_engine.url
    store = PostgresExecutorDispatchStore(
        host=str(url.host),
        port=str(url.port),
        dbname=str(url.database),
        user=str(url.username),
        password=str(url.password),
    )
    supervisor = ExecutorSupervisor(
        cache_dir=tmp_path, artifact_bucket="releases", artifact_prefix="executors", authority_check_interval=0.01
    )
    monkeypatch.setattr(host_supervisor, "ECS_AGENT_URI", "")
    monkeypatch.setattr(host_supervisor, "_AUTHORITY_LOSS_GRACE_SECONDS", 0.01)
    original_is_current = store.is_current
    held_snapshots: list[dict[str, Any]] = []

    async def check_then_hold(authority: DispatchAuthority) -> bool:
        current = await original_is_current(authority)
        if hold_after_poll and current and marker.exists() and not held_snapshots:
            acquire_hold(postgres_session, identity=identity, scope=scope, purpose="deletion")
            postgres_session.commit()
            for row in (run, dispatch, task):
                postgres_session.refresh(row)
                held_snapshots.append(row.model_dump())
            postgres_session.commit()
        return current

    monkeypatch.setattr(store, "is_current", check_then_hold)
    payload = ExecutorProcessPayload.from_payload(
        {"benchmark_id_str": str(run.id), "start_benchmark_request_json": {}, "verified_task_ids": [task.task_id]},
        telemetry_context={"request_id": "", "trace_headers": {}},
    )
    expected_error = DispatchAuthorityLostError if hold_after_poll else RuntimeError
    async with asyncio.timeout(5):
        with pytest.raises(expected_error):
            await run_executor_dispatch(
                supervisor,
                store,
                executor_dispatch_id=str(dispatch.id),
                dispatch=artifact,
                process_payload=payload,
                heartbeat_interval_seconds=3600,
            )
    for row in (run, dispatch, task):
        postgres_session.refresh(row)
    assert dispatch.process_exited_at is not None
    if hold_after_poll:
        assert len(held_snapshots) == 3
        assert run.model_dump() == held_snapshots[0]
        assert dispatch.model_dump(exclude={"process_exited_at"}) == {
            key: value for key, value in held_snapshots[1].items() if key != "process_exited_at"
        }
        assert task.model_dump() == held_snapshots[2]
    else:
        assert run.status == BenchmarkStatus.ERROR
        assert dispatch.status == ExecutorDispatchStatus.FAILED
        assert task.status == TaskStatus.ERROR


@pytest.mark.parametrize("keep_dispatch", [True, False])
def test_drain_rechecks_observation_age_and_retains_hold(postgres_session: Session, keep_dispatch: bool) -> None:
    identity, scope, _, dispatch, _ = seeded_run(postgres_session)
    dispatch.status = ExecutorDispatchStatus.QUEUED
    dispatch.started_at = None
    postgres_session.add(dispatch)
    if not keep_dispatch:
        postgres_session.delete(dispatch)
    record = acquire_hold(postgres_session, identity=identity, scope=scope, purpose="deletion")
    postgres_session.commit()
    observed_at = datetime.now(UTC)
    observation = HostContractObservation(
        contract="stable-host-lifecycle-v1",
        deployment_sha256="a" * 64,
        host_inventory=("host-1",),
        observed_at=observed_at,
        acknowledgement_required_since=observed_at - timedelta(days=1),
        verifier="operator",
    )
    fresh = verify_drain(
        postgres_session,
        identity=identity,
        scope=scope,
        purpose="deletion",
        host_contract=observation,
        now=observed_at + timedelta(minutes=15),
    )
    assert [item.provenance for item in fresh] == (["held_unclaimed"] if keep_dispatch else [])
    with pytest.raises(LifecycleConflict, match="fresh operator inspection"):
        verify_drain(
            postgres_session,
            identity=identity,
            scope=scope,
            purpose="deletion",
            host_contract=observation,
            now=observed_at + timedelta(minutes=15, microseconds=1),
        )
    postgres_session.refresh(record)
    assert record.released_at is None
    assert record.phase == "held"
