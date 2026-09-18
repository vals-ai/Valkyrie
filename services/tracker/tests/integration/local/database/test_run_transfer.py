"""Paired database transfer preserves private raw history and exact scope."""

import asyncio
import json
import os
import re
import subprocess
import sys
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlmodel import Session, SQLModel, create_engine

from tests.factories import make_benchmark
from tests.integration.local.database.test_run_purge import MemoryBoundary, contract
from tests.integration.local.database.test_run_relocation import execute, seed
from tests.transfer_support import FakeTransferBoundary, SecretMetadataSession, transfer_request
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    EvaluationResult,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    FinalEvaluation,
    Org,
    RunLifecycle,
    Task,
    TaskBreakdown,
    TaskStatus,
)
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope, require_unheld
from tracker.lifecycle_completion import RelocationCheckpoint, capture_predecessor
from tracker.run_purge import PurgeOperator, build_plan
from tracker.run_transfer import TransferOperator, cli
from tracker.run_transfer.contracts import TransferCheckpoint, TransferRequest, TransferRun
from tracker.run_transfer.providers import TransferAWSBoundary
from tracker.run_transfer.rows import RowClosure, digest


@pytest.fixture
def pair(request: pytest.FixtureRequest) -> Generator[tuple[Session, Session], None, None]:
    supplied = [os.getenv(f"TRANSFER_TEST_{side}_DATABASE_URL") for side in ("SOURCE", "DESTINATION")]
    if any(value is not None for value in supplied) and not all(supplied):
        raise ValueError("Supply both private transfer database URLs together")

    administrator = None
    owned_names: list[str] = []
    urls: list[URL]
    if all(supplied):
        urls = [make_url(value) for value in supplied if value is not None]
    else:
        container = request.getfixturevalue("postgres_container")
        base_url = make_url(container.get_connection_url())
        administrator = create_engine(base_url, isolation_level="AUTOCOMMIT")
        urls = []
        try:
            with administrator.connect() as connection:
                for _ in range(2):
                    name = "tracker_transfer_test_" + uuid4().hex
                    connection.execute(text(f'CREATE DATABASE "{name}"'))
                    owned_names.append(name)
                    urls.append(base_url.set(database=name))
        except BaseException:
            with administrator.connect() as connection:
                for name in owned_names:
                    connection.execute(text(f'DROP DATABASE "{name}"'))
            administrator.dispose()
            raise

    if len(set(url.database for url in urls)) != 2 or any(
        re.fullmatch(r"tracker_transfer_test_[a-z0-9_]+", url.database or "") is None for url in urls
    ):
        raise ValueError("Transfer databases must have distinct exact tracker_transfer_test_ names")
    engines = [create_engine(url) for url in urls]
    try:
        for engine in engines:
            SQLModel.metadata.create_all(engine)
        with (
            Session(engines[0], expire_on_commit=False) as source,
            Session(engines[1], expire_on_commit=False) as destination,
        ):
            yield source, destination
    finally:
        try:
            for engine in engines:
                SQLModel.metadata.drop_all(engine)
        finally:
            for engine in engines:
                engine.dispose()
            if administrator is not None:
                try:
                    with administrator.connect() as connection:
                        for name in owned_names:
                            connection.execute(text(f'DROP DATABASE "{name}"'))
                finally:
                    administrator.dispose()


def seed_rows(source: Session, destination: Session) -> tuple[Org, Benchmark, Task]:
    org = Org(id=uuid4(), name=str(uuid4()))
    source.add(org)
    destination.add(Org(id=org.id, name=org.name))
    source.commit()
    destination.commit()
    run = make_benchmark(org_id=org.id, status=BenchmarkStatus.FINISHED)
    run.arguments = run.arguments.model_copy(update={"priority": 3, "queue_pool_id": "private-pool"})
    source.add(run)
    source.commit()
    task = Task(org_id=org.id, benchmark=run.id, task_id="private-task", status=TaskStatus.FINISHED)
    source.add(task)
    source.commit()
    timestamp = datetime(2020, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)
    source.connection().execute(
        text("UPDATE benchmark SET finished_at=:timestamp WHERE id=:id"), {"timestamp": timestamp, "id": run.id}
    )
    source.connection().execute(
        text("UPDATE task SET finished_at=:timestamp WHERE id=:id"), {"timestamp": timestamp, "id": task.id}
    )
    source.commit()
    return org, run, task


def test_raw_closure_preserves_stored_timestamps_excluded_arguments_and_all_rows(pair: tuple[Session, Session]) -> None:

    source, destination = pair
    org, run, _task = seed_rows(source, destination)
    closure = RowClosure.read(source, run.id, org.id)
    closure.insert(destination)
    destination.commit()
    actual = RowClosure.read(destination, run.id, org.id)
    assert actual.sha256 == closure.sha256
    assert actual.rows == closure.rows
    assert actual.rows["benchmark"][0]["arguments"]["priority"] == 3
    assert actual.rows["benchmark"][0]["arguments"]["queue_pool_id"] == "private-pool"
    assert actual.rows["task"][0]["finished_at"].year == 2020
    assert "private-pool" not in json.dumps(actual.summary())


def test_raw_closure_rejects_cross_org_children_and_unknown_columns(pair: tuple[Session, Session]) -> None:

    source, destination = pair
    org, run, task = seed_rows(source, destination)
    other = Org(id=uuid4(), name=str(uuid4()))
    source.add(other)
    source.commit()
    source.connection().execute(text("UPDATE task SET org_id=:org WHERE id=:id"), {"org": other.id, "id": task.id})
    source.commit()
    with pytest.raises(LifecycleConflict, match="organization"):
        RowClosure.read(source, run.id, org.id)
    source.connection().execute(text("UPDATE task SET org_id=:org WHERE id=:id"), {"org": org.id, "id": task.id})
    source.connection().execute(text("ALTER TABLE task ADD COLUMN unknown_payload TEXT"))
    source.commit()
    try:
        with pytest.raises(LifecycleConflict, match="schema"):
            RowClosure.read(source, run.id, org.id)
    finally:
        source.rollback()
        source.connection().execute(text("ALTER TABLE task DROP COLUMN unknown_payload"))
        source.commit()


def test_transfer_plan_is_read_only_and_import_installs_hold_atomically(
    pair: tuple[Session, Session], tmp_path: Path
) -> None:

    source, destination = pair
    org, run, _task = seed_rows(source, destination)
    request = transfer_request(source, destination, org, run)
    boundary = FakeTransferBoundary(tmp_path)
    operator = TransferOperator(source, destination, boundary)
    observation = asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    assert source.get(RunLifecycle, run.id) is None
    assert destination.get(Benchmark, run.id) is None
    request["plan"]["runs"][0]["source_rows_sha256"] = observation.runs[0].source_rows_sha256
    request["action"] = "prepare"
    asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    request["action"] = "import"
    imported = asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    assert imported.runs[0].destination_phase == "transferred"
    assert destination.get(Benchmark, run.id) is not None
    assert destination.get_one(RunLifecycle, run.id).released_at is None
    assert source.get(Benchmark, run.id) is not None
    again = asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    assert again.runs[0].destination_rows_sha256 == imported.runs[0].destination_rows_sha256


@pytest.mark.parametrize("failure", ["archive", "cleanup", "destination_content", "tombstone"])
def test_transfer_failure_retains_source_and_exact_holds(
    pair: tuple[Session, Session], tmp_path: Path, failure: str
) -> None:

    source, destination = pair
    org, run, task = seed_rows(source, destination)
    request = transfer_request(source, destination, org, run)
    boundary = FakeTransferBoundary(tmp_path)
    operator = TransferOperator(source, destination, boundary)

    def execute(action: Any) -> Any:
        request["action"] = action
        request["nonce"] = str(uuid4())
        return asyncio.run(operator.execute(TransferRequest.model_validate(request)))

    observed = execute("plan")
    request["plan"]["runs"][0]["source_rows_sha256"] = observed.runs[0].source_rows_sha256
    execute("prepare")
    if failure == "archive":
        boundary.fail_archive = True
        with pytest.raises(RuntimeError):
            execute("import")
        assert destination.get(Benchmark, run.id) is None
        assert destination.get(RunLifecycle, run.id) is None
        boundary.fail_archive = False
        execute("import")
    elif failure == "tombstone":
        destination.add(
            RunLifecycle(
                run_id=run.id,
                purpose="deletion",
                phase="held",
                acquired_at=datetime.now(UTC),
                identity_json="{}",
                scope_json="{}",
            )
        )
        destination.commit()
        with pytest.raises(LifecycleConflict):
            execute("import")
        assert destination.get(Benchmark, run.id) is None
    else:
        imported = execute("import")
        if failure == "destination_content":
            destination.connection().execute(text("UPDATE task SET task_id='changed' WHERE id=:id"), {"id": task.id})
            destination.commit()
            with pytest.raises(LifecycleConflict):
                execute("inspect")
        else:
            plan = TransferRequest.model_validate(request).plan
            request["parent_completion"] = {
                "operation_id": str(plan.source_identity.operation_id),
                "parent_plan_sha256": plan.source_identity.parent_plan_sha256,
                "child_plan_sha256": plan.sha256,
                "valsmith_commit_sha256": "e" * 64,
                "object_completion_sha256": "f" * 64,
                "destination_rows_sha256": digest(
                    [{"run_id": str(run.id), "sha256": imported.runs[0].destination_rows_sha256}]
                ),
                "archives_sha256": digest([imported.runs[0].archive.model_dump(mode="json")]),
            }
            boundary.fail_cleanup = True
            with pytest.raises(RuntimeError):
                execute("cleanup")
            assert source.get(Benchmark, run.id) is not None
            boundary.fail_cleanup = False
            execute("cleanup")
            assert source.get(Benchmark, run.id) is None
            assert source.get_one(RunLifecycle, run.id).phase == "transferred_source_retired"
            assert source.get_one(RunLifecycle, run.id).released_at is None
            execute("finalize")
            assert destination.get_one(RunLifecycle, run.id).phase == "transferred_history_only"
            assert destination.get_one(RunLifecycle, run.id).released_at is None
            local_identity = plan.destination_identity.model_copy(
                update={
                    "operation_id": uuid4(),
                    "source_aws_account_id": plan.destination_identity.destination_aws_account_id,
                }
            )
            purge_boundary = MemoryBoundary()
            deletion = PurgeOperator(
                destination, build_plan(destination, local_identity), purge_boundary, host_contract=contract()
            )
            assert deletion.plan.runs[0].completed_history is not None
            history = asyncio.run(deletion.inspect(request_nonce=uuid4())).runs[0]
            assert history.state == "present_history_held"
            asyncio.run(deletion.prepare())
            purge_boundary.fenced = True
            asyncio.run(deletion.purge())
            assert destination.get(Benchmark, run.id) is None
            assert destination.get_one(RunLifecycle, run.id).purpose == "deletion"
            assert source.get_one(RunLifecycle, run.id).phase == "transferred_source_retired"
            with pytest.raises(LifecycleConflict):
                execute("finalize")
            return
    assert source.get(Benchmark, run.id) is not None
    assert source.get_one(RunLifecycle, run.id).released_at is None


def test_real_cli_plan_is_private_and_creates_no_hold_or_journal(pair: tuple[Session, Session], tmp_path: Path) -> None:

    source, destination = pair
    org, run, _task = seed_rows(source, destination)
    request = transfer_request(source, destination, org, run)
    request_path, report_path = tmp_path / "request.json", tmp_path / "report.json"
    request_path.write_text(json.dumps(request))
    source.rollback()
    destination.rollback()
    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).resolve().parents[4] / "scripts" / "transfer_run_history.py"),
            "--request",
            str(request_path),
            "--report",
            str(report_path),
            "--source-database-url-env",
            "TRANSFER_TEST_SOURCE_DATABASE_URL",
            "--destination-database-url-env",
            "TRANSFER_TEST_DESTINATION_DATABASE_URL",
            "--expected-source-database-target",
            request["plan"]["source_identity"]["database_target"],
            "--expected-destination-database-target",
            request["plan"]["destination_identity"]["database_target"],
            "--source-aws-profile-env",
            "SOURCE_PROFILE",
            "--destination-aws-profile-env",
            "DESTINATION_PROFILE",
            "--journal-directory",
            str(tmp_path / "journal"),
        ],
        env={
            **os.environ,
            "SOURCE_PROFILE": "unused-source",
            "DESTINATION_PROFILE": "unused-destination",
            "TRANSFER_TEST_SOURCE_DATABASE_URL": source.get_bind().engine.url.render_as_string(hide_password=False),
            "TRANSFER_TEST_DESTINATION_DATABASE_URL": destination.get_bind().engine.url.render_as_string(
                hide_password=False
            ),
        },
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(report_path.read_text())
    assert report["nonce"] == request["nonce"]
    assert "private-pool" not in report_path.read_text()
    assert not (tmp_path / "journal").exists()
    assert source.get(RunLifecycle, run.id) is None
    assert destination.get(RunLifecycle, run.id) is None


@pytest.mark.parametrize("changed", [None, "checkpoint", "identity", "current_location", "foreign_account"])
@pytest.mark.parametrize("released", [False, True])
def test_completed_history_predecessor_cross_account_transition_is_exact(
    pair: tuple[Session, Session], tmp_path: Path, changed: str | None, released: bool
) -> None:

    source, destination = pair
    org, run, _task = seed_rows(source, destination)
    request = transfer_request(source, destination, org, run)
    new_identity = OperationIdentity.model_validate(request["plan"]["source_identity"])
    old_identity = new_identity.model_copy(
        update={"operation_id": uuid4(), "destination_aws_account_id": new_identity.source_aws_account_id}
    )
    scope = RunScope.model_validate(request["plan"]["runs"][0]["source"])
    old_scope_payload = scope.model_dump(mode="json")
    old_scope_payload["original_resources"]["s3_bucket"] = "prior-original-bucket"
    old_scope = RunScope.model_validate(old_scope_payload)
    phase = "released" if released else "relocated_history_only"
    checkpoint = RelocationCheckpoint(
        identity_sha256=digest(old_identity.model_dump(mode="json")),
        scope_sha256=digest(old_scope.model_dump(mode="json")),
        child_plan_sha256="a" * 64,
        execution_arguments_sha256="b" * 64,
        execution_policy="portable" if released else "history_only",
        destination_resources=scope.original_resources,
        dispatch_ids=(),
        copied_objects_sha256=digest([]),
        destination_versions_sha256=digest([]),
        parent_completion_sha256="c" * 64,
        phase=phase,
    )
    record = RunLifecycle(
        run_id=run.id,
        identity_json=old_identity.model_dump_json(),
        scope_json=old_scope.model_dump_json(),
        purpose="relocation",
        phase=phase,
        acquired_at=datetime.now(UTC),
        released_at=datetime.now(UTC) if released else None,
        checkpoint_json=checkpoint.model_dump_json(),
    )
    source.add(record)
    source.commit()
    request["plan"]["runs"][0]["predecessor"] = (
        capture_predecessor(record, old_identity, scope)
        .model_copy(update={"completion_sha256": digest(checkpoint.model_dump(mode="json"))})
        .model_dump(mode="json")
    )
    operator = TransferOperator(source, destination, FakeTransferBoundary(tmp_path))
    observed = asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    request["plan"]["runs"][0]["source_rows_sha256"] = observed.runs[0].source_rows_sha256
    if changed == "checkpoint":
        record.checkpoint_json = checkpoint.model_copy(update={"parent_completion_sha256": "d" * 64}).model_dump_json()
    elif changed == "identity":
        record.identity_json = old_identity.model_copy(update={"parent_plan_sha256": "e" * 64}).model_dump_json()
    elif changed == "current_location":
        record.checkpoint_json = checkpoint.model_copy(
            update={"destination_resources": old_scope.original_resources}
        ).model_dump_json()
    elif changed == "foreign_account":
        record.identity_json = old_identity.model_copy(
            update={"destination_aws_account_id": "333333333333"}
        ).model_dump_json()

    source.add(record)
    source.commit()
    original_record = (record.identity_json, record.scope_json, record.checkpoint_json, record.released_at)
    request["action"] = "prepare"
    if changed:
        with pytest.raises(LifecycleConflict):
            asyncio.run(operator.execute(TransferRequest.model_validate(request)))
        retained = source.get_one(RunLifecycle, run.id)
        assert (
            retained.identity_json,
            retained.scope_json,
            retained.checkpoint_json,
            retained.released_at,
        ) == original_record
    else:
        asyncio.run(operator.execute(TransferRequest.model_validate(request)))
        assert source.get_one(RunLifecycle, run.id).identity_json == new_identity.model_dump_json()

    if not changed or not released:
        with pytest.raises(LifecycleConflict):
            require_unheld(source, run.id)


def test_all_stored_child_sets_and_release_snapshots_survive_beside_unrelated_history(
    pair: tuple[Session, Session], tmp_path: Path
) -> None:

    source, destination = pair
    org, run, task = seed_rows(source, destination)
    source_release = ExecutorRelease(
        id="source-release", artifact_uri="s3://source/release.pex", artifact_digest="a" * 64, protocol_version="3"
    )
    destination_release = ExecutorRelease(
        id="destination-release",
        artifact_uri="s3://destination/release.pex",
        artifact_digest="a" * 64,
        protocol_version="3",
    )
    source.add(source_release)
    destination.add(destination_release)
    source.commit()
    destination.commit()
    breakdown = TaskBreakdown(
        sandbox_build_duration=1.25, agent_run_duration=2.5, evaluation_run_duration=3.75, sandbox_run_duration=4.5
    )
    source.add(breakdown)
    source.flush()
    task.task_breakdown = breakdown.id
    source.add(task)
    source.add(
        EvaluationResult(org_id=org.id, task=task.id, instance_id="global-one", result={"private": [1, {"a": True}]})
    )
    source.add(EvaluationResult(org_id=org.id, task=task.id, instance_id="global-two", result={"private": 2}))
    source.add(
        ErrorResult(
            org_id=org.id, task=task.id, error_message="private-error", retry_scheduled=True, failed_attempt_number=2
        )
    )
    source.add(FinalEvaluation(org_id=org.id, benchmark=run.id, final_score=0.75, properties={"private": "score"}))
    source.add(FinalEvaluation(org_id=org.id, benchmark=run.id, final_score=0.5, properties={"private": "older"}))
    source.add(
        ExecutorDispatch(
            benchmark_id=run.id,
            kind=ExecutorDispatchKind.START,
            status=ExecutorDispatchStatus.FAILED,
            executor_release_id=source_release.id,
            executor_artifact_uri=source_release.artifact_uri,
            executor_artifact_digest=source_release.artifact_digest,
            executor_protocol_version="3",
            started_at=datetime(2020, 1, 1),
            process_exited_at=datetime(2020, 1, 2),
            assigned_task_ids=[task.task_id],
            failure_reason="private-failure",
        )
    )
    source.commit()
    unrelated = make_benchmark(org_id=org.id, status=BenchmarkStatus.FINISHED)
    source.add(unrelated)
    source.commit()
    request = transfer_request(source, destination, org, run)
    request["plan"]["releases"] = [
        {
            "source_id": source_release.id,
            "destination_id": destination_release.id,
            "source_artifact_uri": source_release.artifact_uri,
            "destination_artifact_uri": destination_release.artifact_uri,
            "artifact_digest": source_release.artifact_digest,
            "protocol_version": "3",
        }
    ]
    operator = TransferOperator(source, destination, FakeTransferBoundary(tmp_path))
    observed = asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    request["plan"]["runs"][0]["source_rows_sha256"] = observed.runs[0].source_rows_sha256
    request["action"] = "prepare"
    asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    original = RowClosure.read(source, run.id, org.id)
    request["action"] = "import"
    report = asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    copied = RowClosure.read(destination, run.id, org.id)
    for table in ("task", "taskbreakdown", "evaluationresult", "errorresult", "finalevaluation"):
        assert copied.rows[table] == original.rows[table]
    assert len(copied.rows["evaluationresult"]) == 2
    assert len(copied.rows["finalevaluation"]) == 2
    assert copied.rows["executordispatch"][0]["executor_release_id"] == "destination-release"
    assert copied.rows["executordispatch"][0]["executor_artifact_uri"] == "s3://destination/release.pex"
    assert copied.rows["executordispatch"][0]["process_exited_at"] == datetime(2020, 1, 2)
    assert source.get(Benchmark, unrelated.id) is not None
    assert destination.get(Benchmark, unrelated.id) is None
    assert "private-" not in report.model_dump_json()

    plan = TransferRequest.model_validate(request).plan
    assert report.runs[0].archive is not None
    request["parent_completion"] = {
        "operation_id": str(plan.source_identity.operation_id),
        "parent_plan_sha256": plan.source_identity.parent_plan_sha256,
        "child_plan_sha256": plan.sha256,
        "valsmith_commit_sha256": "e" * 64,
        "object_completion_sha256": "f" * 64,
        "destination_rows_sha256": digest([{"run_id": str(run.id), "sha256": report.runs[0].destination_rows_sha256}]),
        "archives_sha256": digest([report.runs[0].archive.model_dump(mode="json")]),
    }
    request["action"] = "cleanup"
    asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    destination.connection().execute(
        text("UPDATE executorrelease SET artifact_uri='s3://changed/release' WHERE id=:id"),
        {"id": destination_release.id},
    )
    destination.commit()
    request["action"] = "finalize"
    with pytest.raises(LifecycleConflict, match="catalog"):
        asyncio.run(operator.execute(TransferRequest.model_validate(request)))


def test_global_instance_conflict_refuses_destination_without_modifying_source(pair: tuple[Session, Session]) -> None:

    source, destination = pair
    org, run, task = seed_rows(source, destination)
    other = make_benchmark(org_id=org.id, status=BenchmarkStatus.FINISHED)
    destination.add(other)
    destination.commit()
    other_task = Task(org_id=org.id, benchmark=other.id, task_id="other", status=TaskStatus.STOPPED)
    destination.add(other_task)
    destination.commit()
    source.add(EvaluationResult(org_id=org.id, task=task.id, instance_id="global-conflict"))
    destination.add(EvaluationResult(org_id=org.id, task=other_task.id, instance_id="global-conflict"))
    source.commit()
    destination.commit()
    closure = RowClosure.read(source, run.id, org.id)
    with pytest.raises(LifecycleConflict, match="global instance"):
        closure.insert(destination)
    destination.rollback()
    assert RowClosure.read(source, run.id, org.id).sha256 == closure.sha256
    assert destination.get(Benchmark, run.id) is None


def test_crash_after_destination_commit_resumes_exact_source_checkpoint(
    pair: tuple[Session, Session], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:

    source, destination = pair
    org, run, _task = seed_rows(source, destination)
    request = transfer_request(source, destination, org, run)
    operator = TransferOperator(source, destination, FakeTransferBoundary(tmp_path))
    observed = asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    request["plan"]["runs"][0]["source_rows_sha256"] = observed.runs[0].source_rows_sha256
    request["action"] = "prepare"
    asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    save = getattr(operator, "_save")

    def crash(session: Session, record: RunLifecycle, checkpoint: TransferCheckpoint) -> None:
        if session is source and checkpoint.phase == "transferred":
            raise RuntimeError("simulated crash")
        save(session, record, checkpoint)

    monkeypatch.setattr(operator, "_save", crash)
    request["action"] = "import"
    with pytest.raises(RuntimeError):
        asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    assert source.get(Benchmark, run.id) is not None
    assert source.get_one(RunLifecycle, run.id).phase == "prepared"
    assert destination.get(Benchmark, run.id) is not None
    assert destination.get_one(RunLifecycle, run.id).phase == "transferred"
    monkeypatch.setattr(operator, "_save", save)
    response = asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    assert response.runs[0].source_phase == "transferred"


def test_explicit_reference_edit_is_exact_and_preserves_source_payload(
    pair: tuple[Session, Session], tmp_path: Path
) -> None:

    source, destination = pair
    org, run, _task = seed_rows(source, destination)
    request = transfer_request(source, destination, org, run)
    request["plan"]["runs"][0]["reference_edits"] = [
        {
            "pointer": "/arguments/sandbox_provider_secret_name",
            "original_sha256": digest("source-provider"),
            "replacement": "destination-provider",
        }
    ]
    operator = TransferOperator(source, destination, FakeTransferBoundary(tmp_path))
    observed = asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    request["plan"]["runs"][0]["source_rows_sha256"] = observed.runs[0].source_rows_sha256
    request["action"] = "prepare"
    asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    request["action"] = "import"
    asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    assert (
        RowClosure.read(source, run.id, org.id).rows["benchmark"][0]["arguments"]["sandbox_provider_secret_name"]
        == "source-provider"
    )
    assert (
        RowClosure.read(destination, run.id, org.id).rows["benchmark"][0]["arguments"]["sandbox_provider_secret_name"]
        == "destination-provider"
    )


def test_declared_source_archive_is_refused_before_any_hold(pair: tuple[Session, Session], tmp_path: Path) -> None:

    source, destination = pair
    org, run, _task = seed_rows(source, destination)
    request = transfer_request(source, destination, org, run)
    parsed = TransferRequest.model_validate(request)
    boundary = FakeTransferBoundary(tmp_path)
    run.log_history = asyncio.run(boundary.archive(parsed, parsed.plan.runs[0])).reference
    source.add(run)
    source.commit()
    with pytest.raises(LifecycleConflict, match="Declared source archive"):
        asyncio.run(TransferOperator(source, destination, boundary).execute(parsed))
    assert source.get(RunLifecycle, run.id) is None
    assert destination.get(RunLifecycle, run.id) is None
    assert destination.get(Benchmark, run.id) is None


def test_portable_destination_releases_only_after_separate_cleanup(
    pair: tuple[Session, Session], tmp_path: Path
) -> None:
    metadata = SecretMetadataSession(
        {
            name: {
                "ARN": f"arn:aws:secretsmanager:us-west-2:222222222222:secret:{name}-ABC123",
                "VersionIdsToStages": {"version-one": ["AWSCURRENT"]},
            }
            for name in ("source-provider", "agent-reference")
        }
    )
    real_boundary = TransferAWSBoundary(None, None, tmp_path, source_session=object(), destination_session=metadata)

    class AvailableBoundary(FakeTransferBoundary):
        async def portable(self, request: TransferRequest, run: TransferRun, rows: RowClosure) -> None:
            await real_boundary.portable(request, run, rows)

    source, destination = pair
    org, run, _task = seed_rows(source, destination)
    contract = AgentContractRequest(name="test", secrets={"API_KEY": "agent-reference"})
    run.arguments = run.arguments.model_copy(update={"contract": contract})
    source.add(run)
    source.commit()
    request = transfer_request(source, destination, org, run)
    request["plan"]["runs"][0]["execution_policy"] = "portable"
    operator = TransferOperator(source, destination, AvailableBoundary(tmp_path))

    def execute(action: Any) -> Any:
        request["action"] = action
        return asyncio.run(operator.execute(TransferRequest.model_validate(request)))

    observed = execute("plan")
    request["plan"]["runs"][0]["source_rows_sha256"] = observed.runs[0].source_rows_sha256
    execute("prepare")
    imported = execute("import")
    assert destination.get_one(Benchmark, run.id).arguments.contract == contract
    assert source.get_one(Benchmark, run.id).arguments.contract == contract
    assert metadata.requested == ["source-provider", "agent-reference"]
    plan = TransferRequest.model_validate(request).plan
    request["parent_completion"] = {
        "operation_id": str(plan.source_identity.operation_id),
        "parent_plan_sha256": plan.source_identity.parent_plan_sha256,
        "child_plan_sha256": plan.sha256,
        "valsmith_commit_sha256": "e" * 64,
        "object_completion_sha256": "f" * 64,
        "destination_rows_sha256": digest(
            [{"run_id": str(run.id), "sha256": imported.runs[0].destination_rows_sha256}]
        ),
        "archives_sha256": digest([imported.runs[0].archive.model_dump(mode="json")]),
    }
    with pytest.raises(LifecycleConflict, match="Separate source cleanup"):
        execute("finalize")
    assert destination.get_one(RunLifecycle, run.id).released_at is None
    execute("cleanup")
    execute("finalize")
    released = destination.get_one(RunLifecycle, run.id).released_at
    assert released is not None
    execute("finalize")
    assert destination.get_one(RunLifecycle, run.id).released_at == released
    assert source.get_one(RunLifecycle, run.id).phase == "transferred_source_retired"
    assert destination.get_one(Benchmark, run.id).arguments.contract == contract


@pytest.mark.parametrize("status", [ExecutorDispatchStatus.QUEUED, ExecutorDispatchStatus.FAILED])
def test_active_or_unproved_legacy_dispatch_blocks_transfer(
    pair: tuple[Session, Session], tmp_path: Path, status: ExecutorDispatchStatus
) -> None:
    source, destination = pair
    org, run, _task = seed_rows(source, destination)
    release = ExecutorRelease(
        id="legacy", artifact_uri="s3://source/release", artifact_digest="a" * 64, protocol_version="3"
    )
    target_release = ExecutorRelease(
        id="legacy", artifact_uri="s3://target/release", artifact_digest="a" * 64, protocol_version="3"
    )
    source.add(release)
    destination.add(target_release)
    source.commit()
    destination.commit()
    source.add(
        ExecutorDispatch(
            benchmark_id=run.id,
            kind=ExecutorDispatchKind.START,
            status=status,
            executor_release_id=release.id,
            executor_artifact_uri=release.artifact_uri,
            executor_artifact_digest=release.artifact_digest,
            executor_protocol_version="3",
            started_at=datetime(2020, 1, 1),
        )
    )
    source.commit()
    request = transfer_request(source, destination, org, run)
    request["plan"]["releases"] = [
        {
            "source_id": "legacy",
            "destination_id": "legacy",
            "source_artifact_uri": release.artifact_uri,
            "destination_artifact_uri": target_release.artifact_uri,
            "artifact_digest": release.artifact_digest,
            "protocol_version": "3",
        }
    ]
    operator = TransferOperator(source, destination, FakeTransferBoundary(tmp_path))
    if status == ExecutorDispatchStatus.QUEUED:
        with pytest.raises(LifecycleConflict):
            asyncio.run(operator.execute(TransferRequest.model_validate(request)))
        assert source.get(RunLifecycle, run.id) is None
    else:
        observed = asyncio.run(operator.execute(TransferRequest.model_validate(request)))
        request["plan"]["runs"][0]["source_rows_sha256"] = observed.runs[0].source_rows_sha256
        request["action"] = "prepare"
        with pytest.raises(LifecycleConflict):
            asyncio.run(operator.execute(TransferRequest.model_validate(request)))
        assert source.get_one(RunLifecycle, run.id).phase == "held"
    assert source.get(Benchmark, run.id) is not None
    assert destination.get(Benchmark, run.id) is None


def test_cli_apply_and_inspect_resume_across_processes_with_fake_providers(
    pair: tuple[Session, Session], tmp_path: Path
) -> None:
    source, destination = pair
    org, run, _task = seed_rows(source, destination)
    request = transfer_request(source, destination, org, run)
    operator = TransferOperator(source, destination, FakeTransferBoundary(tmp_path))
    observed = asyncio.run(operator.execute(TransferRequest.model_validate(request)))
    request["plan"]["runs"][0]["source_rows_sha256"] = observed.runs[0].source_rows_sha256
    script = Path(__file__).resolve().parents[4] / "scripts" / "transfer_run_history.py"
    driver = "import os,sys,runpy; os.environ['DATABASE_URL']=os.environ['TRANSFER_TEST_SOURCE_DATABASE_URL']; import tracker.run_transfer.cli as boundary; from tests.transfer_support import FakeTransferBoundary; from pathlib import Path; script=sys.argv.pop(1); boundary.TransferAWSBoundary=lambda *args: FakeTransferBoundary(Path(script).parent); runpy.run_path(script,run_name='__main__')"
    request_path, report_path = tmp_path / "request.json", tmp_path / "report.json"
    source.rollback()
    destination.rollback()
    for action in ("prepare", "import", "inspect", "import"):
        request["action"] = action
        request["nonce"] = str(uuid4())
        request_path.write_text(json.dumps(request))
        arguments = [
            sys.executable,
            "-c",
            driver,
            str(script),
            "--request",
            str(request_path),
            "--report",
            str(report_path),
            "--source-database-url-env",
            "TRANSFER_TEST_SOURCE_DATABASE_URL",
            "--destination-database-url-env",
            "TRANSFER_TEST_DESTINATION_DATABASE_URL",
            "--expected-source-database-target",
            request["plan"]["source_identity"]["database_target"],
            "--expected-destination-database-target",
            request["plan"]["destination_identity"]["database_target"],
            "--source-aws-profile-env",
            "SOURCE_PROFILE",
            "--destination-aws-profile-env",
            "DESTINATION_PROFILE",
            "--journal-directory",
            str(tmp_path / "journal"),
        ]
        if action != "inspect":
            arguments.append("--apply")
        result = subprocess.run(
            arguments,
            cwd=script.parent.parent,
            env={
                **os.environ,
                "SOURCE_PROFILE": "unused-source",
                "DESTINATION_PROFILE": "unused-destination",
                "TRANSFER_TEST_SOURCE_DATABASE_URL": source.get_bind().engine.url.render_as_string(hide_password=False),
                "TRANSFER_TEST_DESTINATION_DATABASE_URL": destination.get_bind().engine.url.render_as_string(
                    hide_password=False
                ),
            },
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        response = json.loads(report_path.read_text())
        assert response["nonce"] == request["nonce"]
        assert response["runs"][0]["source_identity"] == TransferRequest.model_validate(
            request
        ).plan.source_identity.model_dump(mode="json")
        assert response["runs"][0]["provider_absence"] == "verified_absent"
    assert destination.get(Benchmark, run.id, populate_existing=True) is not None
    assert destination.get_one(RunLifecycle, run.id).released_at is None
    assert source.get(Benchmark, run.id, populate_existing=True) is not None


def test_transfer_cli_in_process_verifies_plan_and_keeps_old_report_on_failure(
    pair: tuple[Session, Session], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source, destination = pair
    org, run, _ = seed_rows(source, destination)
    payload = transfer_request(source, destination, org, run)
    request_path, report_path = tmp_path / "request.json", tmp_path / "report.json"
    request_path.write_text(json.dumps(payload))
    source.rollback()
    destination.rollback()
    arguments = (
        request_path.read_bytes(),
        request_path,
        report_path,
        source.get_bind().engine.url.render_as_string(hide_password=False),
        destination.get_bind().engine.url.render_as_string(hide_password=False),
        payload["plan"]["source_identity"]["database_target"],
        payload["plan"]["destination_identity"]["database_target"],
        "unused-source",
        "unused-destination",
        tmp_path / "journal",
    )
    assert cli.execute(*arguments) == 0
    first = report_path.read_bytes()
    assert json.loads(first)["nonce"] == payload["nonce"]
    assert report_path.stat().st_mode & 0o777 == 0o600
    assert not (tmp_path / "journal").exists()
    assert source.get(RunLifecycle, run.id) is None

    def refuse_replace(self: Path, target: Path) -> Path:
        raise OSError("private write failed")

    monkeypatch.setattr(Path, "replace", refuse_replace)
    with pytest.raises(OSError, match="private write failed"):
        cli.execute(*arguments)
    assert report_path.read_bytes() == first
    assert list(tmp_path.glob(".report.json.*")) == []


def test_actual_same_account_history_then_transfer_keeps_source_held(
    pair: tuple[Session, Session], tmp_path: Path
) -> None:
    source, destination = pair
    run, _, relocation = seed(source)
    execute(source, relocation, "prepare")
    execute(source, relocation, "relocate")
    relocation["completion_sha256"] = "c" * 64
    execute(source, relocation, "release")
    source.refresh(run)
    saved_arguments = run.arguments
    assert saved_arguments.properties is not None
    org = source.get_one(Org, run.org_id)
    destination.add(Org(id=org.id, name=org.name))
    destination.commit()
    payload = transfer_request(source, destination, org, run)
    run.arguments = saved_arguments
    source.add(run)
    source.commit()
    for side in ("source_identity", "destination_identity"):
        payload["plan"][side].update(source_aws_account_id="123456789012", environment="dev")
    payload["plan"]["runs"][0]["source"] = RunScope(
        run_id=run.id, original_resources=saved_arguments.properties
    ).model_dump(mode="json")
    operator = TransferOperator(source, destination, FakeTransferBoundary(tmp_path))
    observation = asyncio.run(operator.execute(TransferRequest.model_validate(payload))).runs[0]
    assert observation.predecessor is not None
    payload["plan"]["runs"][0].update(
        predecessor=observation.predecessor.model_dump(mode="json"), source_rows_sha256=observation.source_rows_sha256
    )
    payload["action"] = "prepare"
    asyncio.run(operator.execute(TransferRequest.model_validate(payload)))
    payload["action"] = "import"
    imported = asyncio.run(operator.execute(TransferRequest.model_validate(payload)))
    assert imported.runs[0].destination_phase == "transferred"
    for session in (source, destination):
        with pytest.raises(LifecycleConflict):
            require_unheld(session, run.id)
    with pytest.raises(LifecycleConflict):
        execute(source, relocation, "release")
