"""Fenced purge behavior against real PostgreSQL and isolated provider state."""

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import Connection, text
from sqlalchemy.exc import DBAPIError
from sqlmodel import Session, select

import tracker.run_purge as run_purge
import tracker.run_purge.cli as purge_cli
import tracker.utils.run_orchestration as orchestration
from tests.factories import make_benchmark, make_task
from tests.integration.local.database.test_lifecycle_holds import seeded_run
from tests.unit.test_purge_locking import FakeLockConnection, scripted_lock
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
from tracker.lifecycle import (
    LifecycleConflict,
    OperationIdentity,
    Purpose,
    RunScope,
    Verification,
    acquire_hold,
    release_relocation_hold,
)
from tracker.lifecycle_evidence import ExternalHostDrain, HostContractObservation
from tracker.run_purge import PurgeOperator, build_plan
from tracker.run_purge.cli import main as purge_cli_main
from tracker.run_purge.contracts import PurgeCheckpoint, PurgePlan
from tracker.run_purge.locking import OperationLock, database_target


_BACKEND_PID = 4242
_HELD = (_BACKEND_PID, 1)
_LOST = (_BACKEND_PID + 1, 1)


def test_cli_apply_failure_and_resume_preserve_durable_proof(
    postgres_session: Session, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    operator, boundary = prepared_operator(postgres_session)
    binding = postgres_session.get_bind()
    engine = binding.engine if isinstance(binding, Connection) else binding
    monkeypatch.setenv("PURGE_CLI_TEST_DATABASE", engine.url.render_as_string(hide_password=False))

    def boundary_factory(*_args: Any, **_kwargs: Any) -> MemoryBoundary:
        return boundary

    monkeypatch.setattr(purge_cli, "AWSProviderBoundary", boundary_factory)
    plan_path, host_path, report_path = (tmp_path / name for name in ("plan.json", "host.json", "report.json"))
    purge_cli.write_plan(plan_path, operator.plan)
    assert operator.host_contract is not None
    host_path.write_text(operator.host_contract.model_dump_json())
    arguments = [
        "--apply",
        "--database-url-env",
        "PURGE_CLI_TEST_DATABASE",
        "--expected-database-target",
        operator.plan.identity.database_target,
        "--plan",
        str(plan_path),
        "--host-contract",
        str(host_path),
        "--report",
        str(report_path),
    ]
    assert purge_cli.main(["prepare", *arguments]) == 0
    assert json.loads(report_path.read_text())["runs"][0]["phase"] == "prepared"
    boundary.fenced = True
    boundary.fail = "logs"
    assert purge_cli.main(["purge", *arguments]) == 2
    failed = json.loads(report_path.read_text())
    assert failed["outcome"] == "incomplete" and failed["runs"][0]["phase"] == "objects_removed"
    assert "logs unavailable" not in capsys.readouterr().err
    boundary.fail = None
    assert purge_cli.main(["resume", *arguments]) == 0
    complete = json.loads(report_path.read_text())
    assert complete["outcome"] == "checked" and complete["runs"][0]["phase"] == "complete"
    assert report_path.stat().st_mode & 0o777 == 0o600
    postgres_session.expire_all()
    run_id = operator.plan.identity.run_ids[0]
    assert postgres_session.get(Benchmark, run_id) is None
    record = postgres_session.get(RunLifecycle, run_id)
    assert record is not None and record.released_at is None

    def fail_report(*_args: Any) -> None:
        raise OSError("private storage error")

    monkeypatch.setattr(purge_cli, "write_report", fail_report)
    assert purge_cli.main(["resume", *arguments]) == 2
    assert "private storage error" not in capsys.readouterr().err
    boundary.objects.add("late")
    assert purge_cli.main(["resume", *arguments]) == 2
    postgres_session.refresh(record)
    assert record.phase == "complete" and record.released_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption",
    [
        "missing",
        "digest",
        "provider",
        "phase",
        "dispatch_scope",
        "pending",
        "exit",
        "unclaimed",
        "finished",
        "external",
        "row_tables",
        "row_duplicates",
        "row_fence",
        "row_run",
        "dispatch_duplicates",
    ],
)
async def test_corrupt_saved_checkpoint_refuses_before_provider_mutation(
    postgres_session: Session, corruption: str
) -> None:
    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    boundary.fail = "objects"
    with pytest.raises(RuntimeError):
        await operator.purge()
    record = postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0])
    assert record is not None and record.checkpoint_json is not None
    checkpoint = json.loads(record.checkpoint_json)
    drain = checkpoint["dispatch_drain"][0]
    if corruption == "digest":
        checkpoint["child_plan_sha256"] = "0" * 64
    elif corruption == "provider":
        checkpoint["provider"]["secret_name"] = "changed"
    elif corruption == "phase":
        checkpoint["phase"] = "objects_removed"
    elif corruption == "dispatch_scope":
        checkpoint["dispatch_drain"] = []
    elif corruption in {"pending", "unclaimed", "finished", "external"}:
        drain["provenance"] = {
            "pending": "pending",
            "unclaimed": "held_unclaimed",
            "finished": "verified_finished_contract",
            "external": "externally_confirmed_host_drain",
        }[corruption]
    elif corruption == "exit":
        drain["observed_exit_at"] = None
    elif corruption == "row_tables":
        checkpoint["rows"] = checkpoint["rows"][:-1]
    elif corruption == "row_duplicates":
        checkpoint["rows"][0]["ids"] *= 2
    elif corruption == "row_fence":
        checkpoint["fence_policy_sha256"] = None
    elif corruption == "row_run":
        next(row for row in checkpoint["rows"] if row["table"] == "benchmark")["ids"] = [str(uuid4())]
    elif corruption == "dispatch_duplicates":
        checkpoint["original_dispatches"] *= 2
    record.checkpoint_json = None if corruption == "missing" else json.dumps(checkpoint)
    postgres_session.add(record)
    postgres_session.commit()
    before = record.model_dump()
    boundary.calls.clear()
    with pytest.raises(LifecycleConflict):
        await operator.purge()
    assert boundary.calls == []
    postgres_session.refresh(record)
    assert record.model_dump() == before and record.released_at is None
    assert postgres_session.get(Benchmark, record.run_id) is not None


@pytest.mark.asyncio
async def test_final_host_observation_expiry_retains_row_checkpoint(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    original_check = boundary.verify_storage_absence

    async def expire_observation(*arguments: Any) -> None:
        await original_check(*arguments)
        if postgres_session.get(Benchmark, operator.plan.identity.run_ids[0]) is None:
            assert operator.host_contract is not None
            operator.host_contract = operator.host_contract.model_copy(
                update={"observed_at": datetime.now(UTC) - timedelta(minutes=16)}
            )

    boundary.verify_storage_absence = expire_observation
    with pytest.raises(LifecycleConflict, match="expired"):
        await operator.purge()
    postgres_session.rollback()
    assert operator.report().runs[0].phase == "rows_removed"
    with pytest.raises(LifecycleConflict, match="expired"):
        operator.report(outcome="checked")
    operator.host_contract = contract()
    boundary.verify_storage_absence = original_check
    assert (await operator.purge()).runs[0].phase == "complete"


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

    async def cleanup_sandboxes(self, *_arguments: Any, verify: Verification) -> None:
        self.calls.append("cleanup")
        if self.fail == "sandbox":
            raise RuntimeError("provider unavailable")

        verify()
        self.sandboxes.clear()

    async def verify_absence(self, *_arguments: Any) -> None:
        self.calls.append("absence")
        if self.sandboxes:
            raise LifecycleConflict("Sandboxes remain")

    async def purge_objects(self, *_arguments: Any, verify: Verification) -> None:
        self.calls.append("objects")
        verify()
        self.objects.discard("version")
        if self.fail == "objects":
            raise RuntimeError("partial S3 failure")
        self.objects.clear()

    async def purge_logs(self, *_arguments: Any, verify: Verification) -> None:
        self.calls.append("logs")
        if self.fail == "logs":
            raise RuntimeError("logs unavailable")

        verify()
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

    async def paused(*arguments: Any, verify: Verification) -> None:
        reached.set()
        await resume.wait()
        await original(*arguments, verify=verify)

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


def relocated_operator(session: Session) -> tuple[PurgeOperator, MemoryBoundary, OperationIdentity, RunScope]:
    operator, boundary = prepared_operator(session)
    original = operator.plan.runs[0].scope
    relocation = operator.plan.identity.model_copy(update={"operation_id": uuid4(), "parent_plan_sha256": "c" * 64})
    acquire_hold(session, identity=relocation, scope=original, purpose="relocation")
    session.commit()
    benchmark = session.get(Benchmark, original.run_id)
    assert benchmark is not None
    destination = replace(original.original_resources, s3_bucket="migrated-owner-data")
    benchmark.arguments = benchmark.arguments.model_copy(update={"properties": destination})
    session.add(benchmark)
    session.commit()

    def verify_completion(current_session: Session, record: RunLifecycle) -> None:
        current_session.refresh(benchmark)
        assert benchmark.arguments.properties == destination
        assert record.purpose == "relocation" and record.released_at is None

    release_relocation_hold(session, identity=relocation, scope=original, verify_completion=verify_completion)
    session.commit()
    return (
        PurgeOperator(session, build_plan(session, operator.plan.identity), boundary, host_contract=contract()),
        boundary,
        relocation,
        original,
    )


@pytest.mark.asyncio
async def test_prepare_replaces_exact_completed_relocation_and_resumes(postgres_session: Session) -> None:
    operator, boundary, relocation, original = relocated_operator(postgres_session)
    planned = operator.plan.runs[0]
    assert planned.scope.original_resources.s3_bucket == "migrated-owner-data"
    assert original.original_resources.s3_bucket == "owner-data"
    report = await operator.prepare()
    assert report.runs[0].phase == "prepared"
    record = postgres_session.get(RunLifecycle, original.run_id)
    assert record is not None and record.purpose == "deletion" and record.released_at is None
    assert OperationIdentity.model_validate_json(record.identity_json) == operator.plan.identity
    checkpoint = PurgeCheckpoint.model_validate_json(record.checkpoint_json or "null")
    assert checkpoint.released_relocation == planned.released_relocation
    assert checkpoint.released_relocation is not None
    assert checkpoint.released_relocation.operation_id == relocation.operation_id
    await operator.prepare()
    boundary.fenced = True
    await operator.purge()
    await operator.purge()


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement_purpose", ["relocation", "deletion"])
async def test_stale_relocation_plan_cannot_replace_new_active_owner(
    postgres_session: Session, replacement_purpose: Purpose
) -> None:
    operator, boundary, relocation, original = relocated_operator(postgres_session)
    cached = postgres_session.get(RunLifecycle, original.run_id)
    assert cached is not None and cached.released_at is not None
    replacement = operator.plan.identity.model_copy(update={"operation_id": uuid4()})
    with Session(postgres_session.get_bind()) as concurrent:
        record = acquire_hold(
            concurrent,
            identity=replacement,
            scope=operator.plan.runs[0].scope,
            purpose=replacement_purpose,
            replace_released_operation_id=relocation.operation_id,
        )
        concurrent.commit()
        concurrent.refresh(record)
        saved = record.model_dump()
    assert cached.released_at is not None
    with pytest.raises(LifecycleConflict):
        await operator.prepare()
    assert "cleanup" not in boundary.calls
    postgres_session.refresh(cached)
    assert cached.model_dump() == saved


@pytest.mark.asyncio
async def test_stale_plan_cannot_replace_different_released_relocation(postgres_session: Session) -> None:
    operator, boundary, relocation, original = relocated_operator(postgres_session)
    replacement = operator.plan.identity.model_copy(update={"operation_id": uuid4()})
    scope = operator.plan.runs[0].scope
    with Session(postgres_session.get_bind()) as concurrent:
        record = acquire_hold(
            concurrent,
            identity=replacement,
            scope=scope,
            purpose="relocation",
            replace_released_operation_id=relocation.operation_id,
        )
        release_relocation_hold(
            concurrent, identity=replacement, scope=scope, verify_completion=lambda _session, _record: None
        )
        concurrent.commit()
        concurrent.refresh(record)
        saved = record.model_dump()
    with pytest.raises(LifecycleConflict):
        await operator.prepare()
    assert "cleanup" not in boundary.calls
    record = postgres_session.get(RunLifecycle, original.run_id)
    assert record is not None and record.model_dump() == saved


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["identity", "scope", "release_time"])
async def test_predecessor_facts_changed_after_plan_are_refused(postgres_session: Session, changed: str) -> None:
    operator, boundary, _, original = relocated_operator(postgres_session)
    with Session(postgres_session.get_bind()) as concurrent:
        record = concurrent.get(RunLifecycle, original.run_id)
        assert record is not None and record.released_at is not None
        if changed == "identity":
            previous = OperationIdentity.model_validate_json(record.identity_json)
            record.identity_json = previous.model_copy(update={"parent_plan_sha256": "f" * 64}).model_dump_json()
        elif changed == "scope":
            previous_scope = RunScope.model_validate_json(record.scope_json)
            record.scope_json = previous_scope.model_copy(
                update={
                    "original_resources": replace(
                        previous_scope.original_resources, s3_bucket="different-original-bucket"
                    )
                }
            ).model_dump_json()
        else:
            record.released_at += timedelta(seconds=1)
        concurrent.add(record)
        concurrent.commit()
        concurrent.refresh(record)
        saved = record.model_dump()
    with pytest.raises(LifecycleConflict):
        await operator.prepare()
    assert "cleanup" not in boundary.calls
    record = postgres_session.get(RunLifecycle, original.run_id)
    assert record is not None and record.model_dump() == saved


@pytest.mark.parametrize("purpose", ["relocation", "deletion"])
def test_plan_refuses_active_or_permanent_predecessor(postgres_session: Session, purpose: Purpose) -> None:
    operator, _ = prepared_operator(postgres_session)
    previous = operator.plan.identity.model_copy(update={"operation_id": uuid4()})
    scope = operator.plan.runs[0].scope
    record = acquire_hold(postgres_session, identity=previous, scope=scope, purpose=purpose)
    postgres_session.commit()
    postgres_session.refresh(record)
    original = record.model_dump()
    with pytest.raises(LifecycleConflict, match="released relocation"):
        build_plan(postgres_session, operator.plan.identity)
    postgres_session.rollback()
    postgres_session.refresh(record)
    assert record.model_dump() == original


@pytest.mark.asyncio
async def test_unplanned_released_predecessor_is_not_automatically_replaced(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    scope = operator.plan.runs[0].scope
    relocation = operator.plan.identity.model_copy(update={"operation_id": uuid4()})
    record = acquire_hold(postgres_session, identity=relocation, scope=scope, purpose="relocation")
    release_relocation_hold(
        postgres_session, identity=relocation, scope=scope, verify_completion=lambda _session, _record: None
    )
    postgres_session.commit()
    postgres_session.refresh(record)
    original = record.model_dump()
    with pytest.raises(LifecycleConflict, match="predecessor changed"):
        await operator.prepare()
    assert "cleanup" not in boundary.calls
    postgres_session.refresh(record)
    assert record.model_dump() == original


@pytest.mark.asyncio
async def test_matching_deletion_resume_preserves_legacy_plan_digest(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    legacy_plan = operator.plan.model_dump(mode="json")
    for run in legacy_plan["runs"]:
        run.pop("released_relocation", None)
        run.pop("abandoned_deletion", None)
        run.pop("expected_run_label", None)
    legacy_digest = hashlib.sha256(json.dumps(legacy_plan, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    await operator.prepare()
    record = postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0])
    assert record is not None and record.checkpoint_json is not None
    legacy_checkpoint = json.loads(record.checkpoint_json)
    legacy_checkpoint.pop("released_relocation", None)
    legacy_checkpoint["child_plan_sha256"] = legacy_digest
    record.checkpoint_json = json.dumps(legacy_checkpoint)
    postgres_session.add(record)
    postgres_session.commit()
    await operator.prepare()
    boundary.fenced = True
    report = await operator.purge()
    assert report.child_plan_sha256 == legacy_digest and report.outcome == "checked"


@pytest.mark.asyncio
async def test_multiple_runs_use_their_own_released_predecessors(postgres_session: Session) -> None:
    first, boundary, first_relocation, _ = relocated_operator(postgres_session)
    second = make_benchmark(org_id=first.plan.identity.org_id)
    second_resources = replace(first.plan.runs[0].scope.original_resources, s3_bucket="second-original-bucket")
    second.arguments = second.arguments.model_copy(
        update={"properties": second_resources, "sandbox_provider_secret_name": "second-provider"}
    )
    postgres_session.add(second)
    postgres_session.commit()
    scope = RunScope(run_id=second.id, original_resources=second_resources)
    second_relocation = first.plan.identity.model_copy(update={"operation_id": uuid4(), "run_ids": (second.id,)})
    acquire_hold(postgres_session, identity=second_relocation, scope=scope, purpose="relocation")
    second.arguments = second.arguments.model_copy(
        update={"properties": replace(second_resources, s3_bucket="second-destination-bucket")}
    )
    postgres_session.add(second)
    postgres_session.commit()
    release_relocation_hold(
        postgres_session, identity=second_relocation, scope=scope, verify_completion=lambda _session, _record: None
    )
    postgres_session.commit()
    identity = first.plan.identity.model_copy(
        update={"run_ids": tuple(sorted((*first.plan.identity.run_ids, second.id), key=str))}
    )
    plan = build_plan(postgres_session, identity)
    predecessors = {run.released_relocation.operation_id for run in plan.runs if run.released_relocation is not None}
    assert predecessors == {first_relocation.operation_id, second_relocation.operation_id}
    operator = PurgeOperator(postgres_session, plan, boundary, host_contract=contract())
    report = await operator.prepare()
    assert tuple(run.phase for run in report.runs) == ("prepared", "prepared")
    for run in plan.runs:
        record = postgres_session.get(RunLifecycle, run.scope.run_id)
        assert record is not None and record.checkpoint_json is not None
        assert (
            PurgeCheckpoint.model_validate_json(record.checkpoint_json).released_relocation == run.released_relocation
        )


def test_cli_plan_reads_released_predecessor_without_mutating_hold(
    postgres_session: Session, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    operator, _, _, original = relocated_operator(postgres_session)
    record = postgres_session.get(RunLifecycle, original.run_id)
    assert record is not None
    postgres_session.refresh(record)
    before = record.model_dump()
    identity_path = tmp_path / "identity.json"
    plan_path = tmp_path / "plan.json"
    identity_path.write_text(operator.plan.identity.model_dump_json())
    binding = postgres_session.get_bind()
    engine = binding.engine if isinstance(binding, Connection) else binding
    monkeypatch.setenv("PURGE_FIX_TEST_DATABASE", engine.url.render_as_string(hide_password=False))
    result = purge_cli_main(
        [
            "--database-url-env",
            "PURGE_FIX_TEST_DATABASE",
            "--expected-database-target",
            operator.plan.identity.database_target,
            "--identity",
            str(identity_path),
            "--plan",
            str(plan_path),
        ]
    )
    assert result == 0
    plan = PurgePlan.model_validate_json(plan_path.read_text())
    assert plan.runs[0].released_relocation == operator.plan.runs[0].released_relocation
    assert plan.digest() == operator.plan.digest()
    assert plan_path.stat().st_mode & 0o777 == 0o600
    postgres_session.refresh(record)
    assert record.model_dump() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("minutes", [-16, 1])
async def test_absent_run_resume_rejects_expired_or_future_host_observation(
    postgres_session: Session, minutes: int
) -> None:
    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    await operator.purge()
    run_id = operator.plan.identity.run_ids[0]
    record = postgres_session.get(RunLifecycle, run_id)
    assert record is not None
    original = record.model_dump()
    assert operator.host_contract is not None
    operator.host_contract = operator.host_contract.model_copy(
        update={"observed_at": datetime.now(UTC) + timedelta(minutes=minutes)}
    )
    boundary.calls.clear()
    with pytest.raises(LifecycleConflict, match="expired"):
        await operator.purge()
    assert boundary.calls == []
    assert postgres_session.get(Benchmark, run_id) is None
    postgres_session.refresh(record)
    assert record.model_dump() == original


@pytest.mark.asyncio
async def test_late_delete_marker_after_row_removal_keeps_purge_incomplete(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    original_verify = boundary.verify_storage_absence

    async def late_marker(*arguments: Any) -> None:
        if postgres_session.get(Benchmark, operator.plan.identity.run_ids[0]) is None:
            boundary.objects.add("late-delete-marker")
        await original_verify(*arguments)

    boundary.verify_storage_absence = late_marker
    with pytest.raises(LifecycleConflict, match="Storage remains"):
        await operator.purge()
    record = postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0])
    assert record is not None and record.phase == "rows_removed"
    assert operator.report().outcome == "incomplete"
    boundary.verify_storage_absence = original_verify
    boundary.objects.clear()
    assert (await operator.purge()).outcome == "checked"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_call", "surviving"),
    [("cleanup", "sandboxes"), ("objects", "objects"), ("logs", "logs")],
)
async def test_a_lock_lost_inside_a_provider_call_stops_that_provider_mutation(
    postgres_session: Session, monkeypatch: pytest.MonkeyPatch, provider_call: str, surviving: str
) -> None:
    """Each mutation reads first, so the operator must carry its verification into the provider."""
    connection = FakeLockConnection(live=_HELD)
    lock = OperationLock(connection, _BACKEND_PID, (11,))

    class LosingBoundary(MemoryBoundary):
        def _lose(self, name: str) -> None:
            if name == provider_call:
                connection.live = _LOST

        async def cleanup_sandboxes(self, *arguments: Any, verify: Verification) -> None:
            self._lose("cleanup")
            await super().cleanup_sandboxes(*arguments, verify=verify)

        async def purge_objects(self, *arguments: Any, verify: Verification) -> None:
            self._lose("objects")
            await super().purge_objects(*arguments, verify=verify)

        async def purge_logs(self, *arguments: Any, verify: Verification) -> None:
            self._lose("logs")
            await super().purge_logs(*arguments, verify=verify)

    operator, _ = prepared_operator(postgres_session)
    boundary = LosingBoundary()
    operator.boundary = boundary
    monkeypatch.setattr(run_purge, "exclusive_operation", lambda *_arguments: scripted_lock(lock))

    if provider_call == "cleanup":
        with pytest.raises(LifecycleConflict, match="advisory lock is no longer held"):
            await operator.prepare()
    else:
        await operator.prepare()
        boundary.fenced = True

        with pytest.raises(LifecycleConflict, match="advisory lock is no longer held"):
            await operator.purge()
    postgres_session.rollback()

    assert getattr(boundary, surviving)
