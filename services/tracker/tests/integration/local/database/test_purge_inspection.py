"""Read-only deletion inspection and labels under PostgreSQL mutation locks."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from jsonschema import validate as validate_schema
from sqlalchemy import Connection
from sqlmodel import Session, select

import tracker.run_purge.cli as cli
from tests.factories import make_benchmark
from tests.integration.local.database.test_run_purge import prepared_operator, relocated_operator
from tracker.database.models import Benchmark, ExecutorDispatch, RunLifecycle
from tracker.lifecycle import LifecycleConflict
from tracker.lifecycle_evidence import ExternalHostDrain
from tracker.run_purge import PurgeOperator, build_plan
from tracker.run_purge.contracts import (
    PresentHeldInspection,
    PresentUnheldInspection,
    PurgeInspection,
    PurgePlan,
    RemovedInspection,
)

DOCS = Path(__file__).resolve().parents[6] / "docs" / "deployment"


@pytest.mark.asyncio
async def test_changed_bound_label_stops_prepare_before_hold(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    payload = operator.plan.model_dump(mode="json")
    payload["runs"][0]["expected_run_label"] = "expected"
    operator.plan = PurgePlan.model_validate(payload)
    benchmark = postgres_session.get(Benchmark, operator.plan.identity.run_ids[0])
    assert benchmark is not None
    benchmark.label = "changed"
    postgres_session.add(benchmark)
    postgres_session.commit()
    with pytest.raises(LifecycleConflict, match="label"):
        await operator.prepare()
    assert postgres_session.get(RunLifecycle, benchmark.id) is None
    assert "cleanup" not in boundary.calls


@pytest.mark.asyncio
async def test_inspection_observes_unheld_without_writes(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    report = await operator.inspect(request_nonce=operator.plan.identity.operation_id)
    assert report.runs[0].state == "present_unheld"
    assert postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0]) is None
    assert boundary.calls == ["validate"]


def bind_label(operator: PurgeOperator, session: Session, label: str = "owner-label") -> None:
    benchmark = session.get(Benchmark, operator.plan.identity.run_ids[0])
    assert benchmark is not None
    benchmark.label = label
    session.add(benchmark)
    session.commit()
    payload = operator.plan.model_dump(mode="json")
    payload["runs"][0]["expected_run_label"] = label
    operator.plan = PurgePlan.model_validate(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["unheld", "held", "prepared", "removed"])
async def test_inspection_states_are_read_only_and_schema_valid(
    postgres_session: Session, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    operator, boundary = prepared_operator(postgres_session)
    bind_label(operator, postgres_session)
    if stage == "held":
        boundary.fail = "sandbox"
        with pytest.raises(RuntimeError):
            await operator.prepare()
    elif stage in {"prepared", "removed"}:
        await operator.prepare()

    if stage == "removed":
        boundary.fenced = True
        await operator.purge()
    record = postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0])
    before = None if record is None else record.model_dump()
    boundary.calls.clear()
    commit = Mock(side_effect=AssertionError("inspection cannot commit"))
    flush = Mock(side_effect=AssertionError("inspection cannot flush"))
    monkeypatch.setattr(postgres_session, "commit", commit)
    monkeypatch.setattr(postgres_session, "flush", flush)
    nonce = uuid4()
    report = await operator.inspect(request_nonce=nonce)
    commit.assert_not_called()
    flush.assert_not_called()
    monkeypatch.undo()
    assert report.request_nonce == nonce
    observed = report.runs[0]
    assert observed.expected_run_label == "owner-label"
    assert observed.state == {"unheld": "present_unheld", "removed": "removed"}.get(stage, "present_held")
    schema = json.loads((DOCS / "tracker-purge-inspection-v1.schema.json").read_text())
    validate_schema(report.model_dump(mode="json"), schema)
    assert not {"cleanup", "objects", "logs"}.intersection(boundary.calls)
    if record is not None:
        postgres_session.refresh(record)
        assert record.model_dump() == before
        assert isinstance(observed, (PresentHeldInspection, RemovedInspection))
        assert (
            observed.checkpoint.checkpoint_sha256 == hashlib.sha256((record.checkpoint_json or "").encode()).hexdigest()
        )
    else:
        assert postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0]) is None


@pytest.mark.asyncio
async def test_inspection_mixed_present_and_removed_progress(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    first = operator.plan.runs[0]
    second = make_benchmark(org_id=operator.plan.identity.org_id)
    second.arguments = second.arguments.model_copy(
        update={
            "properties": first.scope.original_resources,
            "sandbox_provider_secret_name": first.provider.secret_name,
        }
    )
    postgres_session.add(second)
    postgres_session.commit()
    identity = operator.plan.identity.model_copy(
        update={"run_ids": tuple(sorted((*operator.plan.identity.run_ids, second.id), key=str))}
    )
    operator.plan = build_plan(postgres_session, identity)
    await operator.prepare()
    boundary.fenced = True
    original = boundary.purge_objects

    async def stop_second(*arguments: Any) -> None:
        identity, run = arguments
        if run.scope.run_id == operator.plan.runs[1].scope.run_id:
            raise RuntimeError("second run interrupted")
        await original(identity, run)

    boundary.purge_objects = stop_second
    with pytest.raises(RuntimeError):
        await operator.purge()
    records = [postgres_session.get(RunLifecycle, run_id) for run_id in identity.run_ids]
    assert all(record is not None for record in records)
    before = [record.model_dump() for record in records if record is not None]
    report = await operator.inspect(request_nonce=uuid4())
    assert [run.state for run in report.runs] == ["removed", "present_held"]
    for run_id, saved in zip(identity.run_ids, before, strict=True):
        record = postgres_session.get(RunLifecycle, run_id)
        assert record is not None
        postgres_session.refresh(record)
        assert record.model_dump() == saved


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["prepare", "purge", "inspect"])
async def test_fresh_labels_reject_stale_session_rows(postgres_session: Session, stage: str) -> None:
    operator, boundary = prepared_operator(postgres_session)
    bind_label(operator, postgres_session)
    if stage == "purge":
        await operator.prepare()
        boundary.fenced = True
    await operator.inspect(request_nonce=uuid4())
    stale = postgres_session.get(Benchmark, operator.plan.identity.run_ids[0])
    assert stale is not None and stale.label == "owner-label"
    with Session(postgres_session.get_bind()) as concurrent:
        changed = concurrent.get(Benchmark, stale.id)
        assert changed is not None
        changed.label = "changed-after-inventory"
        concurrent.add(changed)
        concurrent.commit()
    boundary.calls.clear()
    with pytest.raises(LifecycleConflict, match="label"):
        if stage == "inspect":
            await operator.inspect(request_nonce=uuid4())
        elif stage == "prepare":
            await operator.prepare()
        else:
            await operator.purge()
    assert not {"cleanup", "objects", "logs"}.intersection(boundary.calls)
    assert boundary.objects and boundary.logs


@pytest.mark.asyncio
async def test_label_change_during_provider_preflight_stops_prepare(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    bind_label(operator, postgres_session)

    async def change_label(*_arguments: object) -> None:
        with Session(postgres_session.get_bind()) as concurrent:
            changed = concurrent.get(Benchmark, operator.plan.identity.run_ids[0])
            assert changed is not None
            changed.label = "changed-at-lock"
            concurrent.add(changed)
            concurrent.commit()

    boundary.validate = change_label
    with pytest.raises(LifecycleConflict, match="label"):
        await operator.prepare()
    assert postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0]) is None
    assert boundary.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption", ["identity", "scope", "provider", "digest", "checkpoint", "phase", "unknown_absence"]
)
async def test_inspection_rejects_unproven_removed_rows(postgres_session: Session, corruption: str) -> None:
    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    await operator.purge()
    record = postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0])
    assert record is not None and record.checkpoint_json is not None
    checkpoint = json.loads(record.checkpoint_json)
    if corruption == "identity":
        identity = json.loads(record.identity_json)
        identity["operation_id"] = str(uuid4())
        record.identity_json = json.dumps(identity)
    elif corruption == "scope":
        scope = json.loads(record.scope_json)
        scope["original_resources"]["s3_bucket"] = "wrong-bucket"
        record.scope_json = json.dumps(scope)
    elif corruption == "provider":
        checkpoint["provider"]["secret_name"] = "wrong-provider"
    elif corruption == "digest":
        checkpoint["child_plan_sha256"] = "0" * 64
    elif corruption == "phase":
        checkpoint["phase"] = "logs_removed"
        record.phase = "logs_removed"
    record.checkpoint_json = None if corruption == "checkpoint" else json.dumps(checkpoint)
    postgres_session.add(record)
    if corruption == "unknown_absence":
        postgres_session.delete(record)
    postgres_session.commit()
    boundary.calls.clear()
    with pytest.raises(LifecycleConflict):
        await operator.inspect(request_nonce=uuid4())
    assert boundary.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["past", "future", "fence", "sandbox", "storage", "provider"])
async def test_removed_inspection_rechecks_current_authority(postgres_session: Session, problem: str) -> None:
    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    await operator.purge()
    record = postgres_session.get(RunLifecycle, operator.plan.identity.run_ids[0])
    assert record is not None
    before = record.model_dump()
    if problem in {"past", "future"}:
        assert operator.host_contract is not None
        operator.host_contract = operator.host_contract.model_copy(
            update={"observed_at": datetime.now(UTC) + timedelta(minutes=-16 if problem == "past" else 1)}
        )
    elif problem == "fence":
        boundary.verify_fence = AsyncMock(return_value="c" * 64)
    elif problem == "sandbox":
        boundary.sandboxes.add("late")
    elif problem == "storage":
        boundary.objects.add("late-marker")
    else:
        boundary.validate = AsyncMock(side_effect=RuntimeError("provider uncertainty"))
    boundary.calls.clear()
    with pytest.raises((LifecycleConflict, RuntimeError)):
        await operator.inspect(request_nonce=uuid4())
    assert not {"cleanup", "objects", "logs"}.intersection(boundary.calls)
    postgres_session.refresh(record)
    assert record.model_dump() == before


@pytest.mark.asyncio
async def test_removed_omitted_label_cannot_become_checked(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    await operator.purge()
    observed = (await operator.inspect(request_nonce=uuid4())).runs[0]
    assert isinstance(observed, RemovedInspection)
    assert observed.expected_run_label is None
    payload = operator.plan.model_dump(mode="json")
    payload["runs"][0]["expected_run_label"] = "invented"
    operator.plan = PurgePlan.model_validate(payload)
    with pytest.raises(LifecycleConflict, match="checkpoint identity"):
        await operator.inspect(request_nonce=uuid4())


def test_real_cli_inspection_no_apply_and_nonce(
    postgres_session: Session, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    operator, boundary = prepared_operator(postgres_session)
    binding = postgres_session.get_bind()
    engine = binding.engine if isinstance(binding, Connection) else binding
    monkeypatch.setenv("TASK6_PRIVATE_DB", engine.url.render_as_string(hide_password=False))
    monkeypatch.setattr(cli, "AWSProviderBoundary", Mock(return_value=boundary))
    plan_path, report_path = tmp_path / "plan.json", tmp_path / "report.json"
    cli.write_plan(plan_path, operator.plan)
    nonce = uuid4()
    arguments = [
        "inspect",
        "--database-url-env",
        "TASK6_PRIVATE_DB",
        "--expected-database-target",
        operator.plan.identity.database_target,
        "--plan",
        str(plan_path),
        "--report",
        str(report_path),
        "--request-nonce",
        str(nonce),
    ]
    assert cli.main(arguments) == 0
    report = PurgeInspection.model_validate_json(report_path.read_text())
    assert report.request_nonce == nonce and report.runs[0].state == "present_unheld"
    assert report_path.stat().st_mode & 0o777 == 0o600
    before = report_path.read_bytes()
    engine_factory = Mock(side_effect=AssertionError("must reject before DB connection"))
    monkeypatch.setattr(cli, "create_engine", engine_factory)
    assert cli.main([*arguments, "--apply"]) == 2
    engine_factory.assert_not_called()
    assert report_path.read_bytes() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["cleanup", "objects", "logs"])
async def test_label_change_after_phase_commit_stops_next_provider_mutation(
    postgres_session: Session, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    operator, boundary = prepared_operator(postgres_session)
    bind_label(operator, postgres_session)
    if mutation != "cleanup":
        await operator.prepare()
        boundary.fenced = True

    boundary.calls.clear()
    original_commit = postgres_session.commit
    commit_count = 0
    target_commit = 1 if mutation == "objects" else 2

    def change_after_commit() -> None:
        nonlocal commit_count
        original_commit()
        commit_count += 1
        if commit_count != target_commit:
            return

        with Session(postgres_session.get_bind()) as concurrent:
            benchmark = concurrent.get(Benchmark, operator.plan.identity.run_ids[0])
            assert benchmark is not None
            benchmark.label = "changed-between-phases"
            concurrent.add(benchmark)
            concurrent.commit()

    monkeypatch.setattr(postgres_session, "commit", change_after_commit)
    with pytest.raises(LifecycleConflict, match="label"):
        await (operator.prepare() if mutation == "cleanup" else operator.purge())
    assert mutation not in boundary.calls
    assert postgres_session.get(Benchmark, operator.plan.identity.run_ids[0]) is not None


@pytest.mark.asyncio
async def test_inspection_does_not_replace_planned_released_hold(postgres_session: Session) -> None:
    operator, boundary, _, original = relocated_operator(postgres_session)
    report = await operator.inspect(request_nonce=uuid4())
    observed = report.runs[0]
    assert isinstance(observed, PresentUnheldInspection)
    assert observed.released_relocation == operator.plan.runs[0].released_relocation
    record = postgres_session.get(RunLifecycle, original.run_id)
    assert record is not None
    assert record.released_at is not None
    assert record.purpose == "relocation"
    assert boundary.calls == ["validate"]


@pytest.mark.asyncio
async def test_inspection_rejects_unplanned_released_hold(postgres_session: Session) -> None:
    operator, _, _, _ = relocated_operator(postgres_session)
    payload = operator.plan.model_dump(mode="json")
    payload["runs"][0]["released_relocation"] = None
    operator.plan = PurgePlan.model_validate(payload)
    with pytest.raises(LifecycleConflict, match="predecessor changed"):
        await operator.inspect(request_nonce=uuid4())


@pytest.mark.asyncio
async def test_removed_inspection_requires_exact_external_drain_evidence(postgres_session: Session) -> None:
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
    evidence = b"task6 local host termination evidence"
    external = ExternalHostDrain(
        provenance="externally_confirmed_host_drain",
        identity=operator.plan.identity,
        run_id=record.run_id,
        hold_acquired_at=record.acquired_at.replace(tzinfo=UTC),
        dispatch_ids=(dispatch.id,),
        host_inventory=operator.host_contract.host_inventory,
        deployed_host_contract=operator.host_contract.contract,
        observed_at=datetime.now(UTC),
        verifier="task6",
        evidence_sha256=hashlib.sha256(evidence).hexdigest(),
        confirmation="all_inventory_hosts_terminated_and_old_claims_disabled",
    )
    operator.external = (external,)
    operator.external_evidence = (evidence,)
    await operator.prepare()
    boundary.fenced = True
    await operator.purge()
    postgres_session.refresh(record)
    before = record.model_dump()
    assert (await operator.inspect(request_nonce=uuid4())).runs[0].state == "removed"
    operator.external_evidence = ()
    with pytest.raises(LifecycleConflict, match="evidence bytes"):
        await operator.inspect(request_nonce=uuid4())
    operator.external_evidence = (evidence,)
    operator.external = ()
    with pytest.raises(LifecycleConflict, match="external drain"):
        await operator.inspect(request_nonce=uuid4())
    postgres_session.refresh(record)
    assert record.model_dump() == before


@pytest.mark.asyncio
async def test_removed_inspection_rechecks_host_freshness_after_provider_calls(postgres_session: Session) -> None:
    operator, boundary = prepared_operator(postgres_session)
    await operator.prepare()
    boundary.fenced = True
    await operator.purge()
    original = boundary.verify_storage_absence

    async def expire_host(*arguments: Any) -> None:
        await original(*arguments)
        assert operator.host_contract is not None
        operator.host_contract = operator.host_contract.model_copy(
            update={"observed_at": datetime.now(UTC) - timedelta(minutes=16)}
        )

    boundary.verify_storage_absence = expire_host
    with pytest.raises(LifecycleConflict, match="expired"):
        await operator.inspect(request_nonce=uuid4())
