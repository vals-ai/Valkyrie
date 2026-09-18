"""Fenced purge behavior against real PostgreSQL and isolated provider state."""

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlmodel import Session, select

import tracker.utils.run_orchestration as orchestration
from tests.factories import make_benchmark, make_task
from tests.integration.local.database.test_lifecycle_holds import seeded_run
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Org,
    RunLifecycle,
    Task,
    TaskBreakdown,
)
from tracker.exceptions import TrackerServiceError
from tracker.lifecycle import LifecycleConflict
from tracker.lifecycle_evidence import ExternalHostDrain, HostContractObservation
from tracker.run_purge import PurgeOperator, build_plan
from tracker.run_purge.locking import database_target


class MemoryBoundary:
    def __init__(self) -> None:
        self.sandboxes: set[str] = {"sandbox"}
        self.objects: set[str] = {"version", "marker", "upload"}
        self.logs = True
        self.fenced = False
        self.fail: str | None = None
        self.calls: list[str] = []

    async def validate(self, *_arguments: Any) -> None:
        self.calls.append("validate")

    async def verify_fence(self, *_arguments: Any) -> str:
        self.calls.append("fence")
        if not self.fenced:
            raise LifecycleConflict("Owner write fence missing")
        return "b" * 64

    async def cleanup_sandboxes(self, *_arguments: Any) -> None:
        self.calls.append("cleanup")
        if self.fail == "sandbox":
            raise RuntimeError("provider unavailable")
        self.sandboxes.clear()

    async def verify_absence(self, *_arguments: Any) -> None:
        self.calls.append("absence")
        if self.sandboxes:
            raise LifecycleConflict("Sandboxes remain")

    async def purge_objects(self, *_arguments: Any) -> None:
        self.calls.append("objects")
        self.objects.discard("version")
        if self.fail == "objects":
            raise RuntimeError("partial S3 failure")
        self.objects.clear()

    async def purge_logs(self, *_arguments: Any) -> None:
        self.calls.append("logs")
        if self.fail == "logs":
            raise RuntimeError("logs unavailable")
        self.logs = False

    async def verify_storage_absence(self, *_arguments: Any) -> None:
        self.calls.append("storage_absence")
        if self.objects or self.logs:
            raise LifecycleConflict("Storage remains")


def contract() -> HostContractObservation:
    return HostContractObservation(
        contract="stable-host-lifecycle-v1",
        deployment_sha256="b" * 64,
        host_inventory=("host-1",),
        observed_at=datetime.now(UTC),
        acknowledgement_required_since=datetime(2026, 1, 1, tzinfo=UTC),
        verifier="operator",
    )


def prepared_operator(session: Session) -> tuple[PurgeOperator, MemoryBoundary]:
    identity, _, run, dispatch, _ = seeded_run(session)
    identity = identity.model_copy(update={"database_target": database_target(session)})
    run.arguments = run.arguments.model_copy(update={"sandbox_provider_secret_name": "provider-locator"})
    dispatch.process_exited_at = datetime.now(UTC)
    session.add(run)
    session.add(dispatch)
    session.commit()
    boundary = MemoryBoundary()
    return PurgeOperator(session, build_plan(session, identity), boundary, host_contract=contract()), boundary


@pytest.mark.asyncio
async def test_prepare_fence_purge_resume_and_unrelated_rows(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    scope = operator.plan.runs[0].scope
    other = make_benchmark(org_id=operator.plan.identity.org_id)
    breakdown = TaskBreakdown()
    postgres_session.add_all([other, breakdown])
    postgres_session.flush()
    target = postgres_session.get(Benchmark, scope.run_id)
    assert target is not None
    target_task, other_task = make_task(target, "one"), make_task(other, "two")
    target_task.task_breakdown = other_task.task_breakdown = breakdown.id
    postgres_session.add_all([target_task, other_task])
    postgres_session.commit()
    target_task_id = target_task.id
    await operator.prepare()
    assert boundary.objects and boundary.logs
    assert postgres_session.get(Benchmark, scope.run_id) is not None
    with pytest.raises(LifecycleConflict, match="fence"):
        await operator.purge()
    assert boundary.objects and boundary.logs
    boundary.fenced = True
    await operator.purge()
    assert postgres_session.get(Benchmark, scope.run_id) is None
    assert postgres_session.get(Task, target_task_id) is None
    assert postgres_session.get(Task, other_task.id) is not None
    assert postgres_session.get(TaskBreakdown, breakdown.id) is not None
    assert postgres_session.get(Org, operator.plan.identity.org_id) is not None
    assert len(postgres_session.exec(select(ExecutorRelease)).all()) == 1
    assert len(postgres_session.exec(select(ExecutorAdmission)).all()) == 1
    record = postgres_session.get(RunLifecycle, scope.run_id)
    assert record is not None and record.phase == "complete" and record.released_at is None
    assert record.checkpoint_json and "provider-locator" in record.checkpoint_json
    await operator.purge()
    boundary.sandboxes.add("late")
    with pytest.raises(LifecycleConflict, match="Sandboxes"):
        await operator.purge()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["sandbox", "objects", "logs"])
async def test_partial_provider_failure_retains_hold_and_resumes(postgres_session: Session, failure: str) -> None:
    operator, boundary = prepared_operator(postgres_session)
    boundary.fail = failure
    if failure != "sandbox":
        await operator.prepare()
        boundary.fenced = True
    with pytest.raises(RuntimeError):
        await (operator.prepare() if failure == "sandbox" else operator.purge())
    record = postgres_session.get(RunLifecycle, operator.plan.runs[0].scope.run_id)
    assert record is not None and record.phase != "complete" and record.released_at is None
    boundary.fail = None
    await operator.prepare()
    boundary.fenced = True
    await operator.purge()
    postgres_session.refresh(record)
    assert record.phase == "complete"


@pytest.mark.asyncio
async def test_failed_dispatch_without_exit_blocks_prepare(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    dispatch = postgres_session.exec(select(ExecutorDispatch)).one()
    dispatch.process_exited_at = None
    postgres_session.add(dispatch)
    postgres_session.commit()
    with pytest.raises(LifecycleConflict, match="drain"):
        await operator.prepare()
    assert boundary.objects and boundary.logs
    assert "absence" not in boundary.calls


@pytest.mark.asyncio
async def test_unknown_fk_refuses_before_storage_removal(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    postgres_session.connection().execute(
        text("CREATE TABLE future_child (id uuid PRIMARY KEY, run_id uuid REFERENCES benchmark(id) ON DELETE CASCADE)")
    )
    postgres_session.commit()
    try:
        await operator.prepare()
        boundary.fenced = True
        with pytest.raises(LifecycleConflict, match="foreign key"):
            await operator.purge()
        assert boundary.objects and boundary.logs
    finally:
        postgres_session.rollback()
        postgres_session.connection().execute(text("DROP TABLE future_child"))
        postgres_session.commit()


@pytest.mark.asyncio
async def test_missing_checkpoint_cannot_prove_absent_run(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    await operator.purge()
    record = postgres_session.get(RunLifecycle, operator.plan.runs[0].scope.run_id)
    assert record is not None
    record.checkpoint_json = None
    postgres_session.add(record)
    postgres_session.commit()
    with pytest.raises(LifecycleConflict, match="checkpoint"):
        await operator.purge()


@pytest.mark.asyncio
async def test_concurrent_purge_refuses_same_operation(postgres_session: Session) -> None:

    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    reached = asyncio.Event()
    resume = asyncio.Event()
    original = boundary.purge_objects

    async def paused(*arguments: Any) -> None:
        reached.set()
        await resume.wait()
        await original(*arguments)

    boundary.purge_objects = paused
    first = asyncio.create_task(operator.purge())
    await reached.wait()
    try:
        with Session(postgres_session.get_bind()) as other_session:
            other = PurgeOperator(other_session, operator.plan, MemoryBoundary(), host_contract=contract())
            with pytest.raises(LifecycleConflict, match="already active"):
                await other.purge()
    finally:
        resume.set()
        await first


@pytest.mark.asyncio
async def test_late_sandbox_after_drain_keeps_prepare_pending(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)

    async def late(*_arguments: Any) -> None:
        boundary.sandboxes.add("late-create")
        raise LifecycleConflict("Sandboxes remain")

    boundary.verify_absence = late
    with pytest.raises(LifecycleConflict, match="Sandboxes"):
        await operator.prepare()
    record = postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0])
    assert record is not None and record.phase == "held"
    assert boundary.objects and boundary.logs


@pytest.mark.asyncio
async def test_sql_failure_keeps_rows_and_resumes(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    postgres_session.connection().execute(
        text(
            "CREATE FUNCTION refuse_purge() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$"
        )
    )
    postgres_session.connection().execute(
        text("CREATE TRIGGER refuse_purge BEFORE DELETE ON benchmark FOR EACH ROW EXECUTE FUNCTION refuse_purge()")
    )
    postgres_session.commit()
    try:
        with pytest.raises(DBAPIError):
            await operator.purge()
        postgres_session.rollback()
        assert postgres_session.get(Benchmark, operator.plan.identity.run_ids[0]) is not None
        record = postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0])
        assert record is not None and record.phase == "logs_removed"
    finally:
        postgres_session.rollback()
        postgres_session.connection().execute(text("DROP TRIGGER refuse_purge ON benchmark"))
        postgres_session.connection().execute(text("DROP FUNCTION refuse_purge"))
        postgres_session.commit()
    await operator.purge()
    assert postgres_session.get(Benchmark, operator.plan.identity.run_ids[0]) is None


def test_plan_rejects_wrong_actual_database_target(postgres_session: Session) -> None:
    identity, _, run, _, _ = seeded_run(postgres_session)
    run.arguments = run.arguments.model_copy(update={"sandbox_provider_secret_name": "provider"})
    postgres_session.add(run)
    postgres_session.commit()
    with pytest.raises(LifecycleConflict, match="database target"):
        build_plan(postgres_session, identity)


@pytest.mark.asyncio
async def test_malformed_completed_checkpoint_cannot_skip_row_postchecks(postgres_session: Session) -> None:

    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    await operator.purge()
    record = postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0])
    assert record is not None and record.checkpoint_json is not None
    saved = json.loads(record.checkpoint_json)
    saved["rows"] = []
    record.checkpoint_json = json.dumps(saved)
    postgres_session.add(record)
    postgres_session.commit()
    with pytest.raises(LifecycleConflict, match="checkpoint"):
        await operator.purge()


@pytest.mark.asyncio
async def test_terminal_run_can_prepare_and_late_payload_cannot_recreate_it(
    postgres_session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:

    operator, boundary = prepared_operator(postgres_session)
    run_id = operator.plan.identity.run_ids[0]
    run = postgres_session.get(Benchmark, run_id)
    dispatch = postgres_session.exec(select(ExecutorDispatch)).one()
    assert run is not None
    run.status = BenchmarkStatus.FINISHED
    dispatch.status = ExecutorDispatchStatus.FINISHED
    dispatch.process_exited_at = None
    postgres_session.add_all([run, dispatch])
    postgres_session.commit()
    # Build fresh preparation proof for a run that was already terminal.
    await operator.prepare()
    boundary.fenced = True
    await operator.purge()
    monkeypatch.setattr(orchestration, "engine", postgres_session.get_bind())
    runtime_setup = AsyncMock()
    monkeypatch.setattr(orchestration, "_parse_queued_execution", runtime_setup)
    with pytest.raises(TrackerServiceError, match="not found"):
        await orchestration.process_benchmark(
            benchmark_id_str=str(run_id), executor_dispatch_id=str(operator.plan.identity.operation_id)
        )
    runtime_setup.assert_not_called()
    assert postgres_session.get(Benchmark, run_id) is None


@pytest.mark.asyncio
async def test_changed_provider_locator_rejects_before_permanent_hold(postgres_session: Session) -> None:
    operator, _ = prepared_operator(postgres_session)
    run_id = operator.plan.identity.run_ids[0]
    run = postgres_session.get(Benchmark, run_id)
    assert run is not None
    run.arguments = run.arguments.model_copy(update={"sandbox_provider_secret_name": "other-provider"})
    postgres_session.add(run)
    postgres_session.commit()
    with pytest.raises(LifecycleConflict, match="scope changed"):
        await operator.prepare()
    assert postgres_session.get(RunLifecycle, run_id) is None


@pytest.mark.asyncio
async def test_legacy_external_proof_is_retained_and_required_after_row_removal(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    dispatch = postgres_session.exec(select(ExecutorDispatch)).one()
    dispatch.started_at = datetime(2025, 1, 1, tzinfo=UTC)
    dispatch.process_exited_at = None
    postgres_session.add(dispatch)
    postgres_session.commit()
    assert operator.host_contract is not None
    operator.host_contract = operator.host_contract.model_copy(update={"legacy_dispatch_ids": (dispatch.id,)})
    with pytest.raises(LifecycleConflict, match="drain"):
        await operator.prepare()
    record = postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0])
    assert record is not None
    evidence = b"named operator host termination evidence"
    external = ExternalHostDrain(
        provenance="externally_confirmed_host_drain",
        identity=operator.plan.identity,
        run_id=record.run_id,
        hold_acquired_at=record.acquired_at.replace(tzinfo=UTC),
        dispatch_ids=(dispatch.id,),
        host_inventory=operator.host_contract.host_inventory,
        deployed_host_contract=operator.host_contract.contract,
        observed_at=datetime.now(UTC),
        verifier="operator",
        evidence_sha256=hashlib.sha256(evidence).hexdigest(),
        confirmation="all_inventory_hosts_terminated_and_old_claims_disabled",
    )
    operator.external = (external,)
    operator.external_evidence = (evidence,)
    await operator.prepare()
    boundary.fenced = True
    report = await operator.purge()
    assert report.runs[0].external_host_drain == external
    assert report.outcome == "checked"
    await operator.purge()
    operator.external_evidence = ()
    with pytest.raises(LifecycleConflict, match="evidence bytes"):
        await operator.purge()
    assert operator.report().outcome == "incomplete"
