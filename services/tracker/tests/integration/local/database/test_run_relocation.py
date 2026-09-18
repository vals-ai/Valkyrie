"""Relocation holds and raw saved argument changes on PostgreSQL."""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlmodel import Session, SQLModel, create_engine

from tests.factories import make_benchmark
from tests.relocation_support import VersionStore
from tracker.aws.runtime import AWSResources
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Org,
    RunLifecycle,
    Task,
    TaskStatus,
)
from tracker.executor.release_control import create_executor_dispatch
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope, require_unheld
from tracker.lifecycle_completion import acquire_successor_hold, capture_predecessor
from tracker.run_relocation import RelocationOperator
from tracker.run_relocation.providers import RelocationAWSBoundary
from tracker.storage_migration_exchange import ExecutionReference, TrackerRequest, TrackerResponse


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@pytest.fixture
def relocation_session(request: pytest.FixtureRequest) -> Generator[Session, None, None]:
    url = os.getenv("RELOCATION_TEST_DATABASE_URL")
    if url is None:
        engine = request.getfixturevalue("postgres_engine")
        with Session(engine, expire_on_commit=False) as session:
            yield session
        return

    engine = create_engine(url)
    assert str(engine.url.database).startswith("tracker_relocation_test_")
    SQLModel.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as session:
        session.add(ExecutorAdmission())
        session.commit()
        yield session
    SQLModel.metadata.drop_all(engine)
    engine.dispose()


class EmptyBoundary:
    async def validate_source(self, *arguments: object) -> None:
        await self.validate(*arguments)

    async def validate(self, *_arguments: object) -> None:
        pass

    async def verify_absence(self, *_arguments: object) -> None:
        pass

    async def cleanup_sandboxes(self, *_arguments: object) -> None:
        pass

    async def verify_objects(
        self, *_arguments: object, source_removed: bool = False, source_partial: bool = False
    ) -> None:
        pass

    async def execution_references(self, *_arguments: object) -> tuple[ExecutionReference, ...]:
        return ()


def seed(session: Session, policy: str = "history_only") -> tuple[Benchmark, dict[str, Any], dict[str, Any]]:
    org = Org(id=uuid4(), name=str(uuid4()))
    session.add(org)
    session.commit()
    run = make_benchmark(org_id=org.id, status=BenchmarkStatus.FINISHED)
    resources = AWSResources("us-east-1", "valsmith-dev-42-source", "runs", 7)
    run.arguments = run.arguments.model_copy(
        update={
            "properties": resources,
            "priority": 3,
            "queue_pool_id": "saved-pool",
            "sandbox_provider_secret_name": "test-provider",
        }
    )
    session.add(run)
    session.commit()
    raw = (
        session.connection().execute(text("SELECT arguments FROM benchmark WHERE id=:id"), {"id": run.id}).scalar_one()
    )
    original = json.loads(json.dumps(raw))
    raw["properties"].pop("s3_bucket")
    identity = {
        "schema_version": 1,
        "operation_id": str(uuid4()),
        "parent_plan_sha256": "a" * 64,
        "github_owner_id": 42,
        "org_id": str(org.id),
        "source_aws_account_id": "123456789012",
        "destination_aws_account_id": "123456789012",
        "region": "us-east-1",
        "environment": "dev",
        "database_target": f"postgresql:{session.get_bind().engine.url.query.get('host', session.get_bind().engine.url.host or 'localhost')}:{session.get_bind().engine.url.port or session.get_bind().engine.url.query.get('port', '5432')}/{session.get_bind().engine.url.database}",
        "run_ids": [str(run.id)],
    }
    request = {
        **{key: value for key, value in identity.items() if key not in {"operation_id", "parent_plan_sha256"}},
        "action": "prepare",
        "nonce": str(uuid4()),
        "plan": {
            "schema_version": 1,
            "identity": identity,
            "runs": [
                {
                    "scope": {
                        "run_id": str(run.id),
                        "original_resources": original["properties"],
                        "object_prefix": f"benchmarks/{run.id}/",
                        "log_group": f"runs/{run.id}",
                    },
                    "destination_resources": {**original["properties"], "s3_bucket": "valsmith-dev-42-destination"},
                    "expected_label": None,
                    "execution_policy": policy,
                    "execution_arguments_sha256": digest(raw),
                }
            ],
        },
        "host_contract": {
            "contract": "stable-host-lifecycle-v1",
            "deployment_sha256": "b" * 64,
            "host_inventory": ["host-1"],
            "observed_at": datetime.now(UTC).isoformat(),
            "acknowledgement_required_since": (datetime.now(UTC) - timedelta(days=1)).isoformat(),
            "verifier": "test-operator",
        },
    }
    return run, original, request


def execute(session: Session, request: dict[str, Any], action: str) -> TrackerResponse:
    request = {**request, "action": action, "nonce": str(uuid4())}
    return asyncio.run(RelocationOperator(session, EmptyBoundary()).execute(TrackerRequest.model_validate(request)))


def test_terminal_relocation_preserves_full_stored_arguments_and_retains_history_hold(
    relocation_session: Session,
) -> None:
    run, original, request = seed(relocation_session)
    execute(relocation_session, request, "prepare")
    response = execute(relocation_session, request, "relocate")
    assert response.runs[0].resources.s3_bucket == "valsmith-dev-42-destination"
    persisted = (
        relocation_session.connection()
        .execute(text("SELECT arguments FROM benchmark WHERE id=:id"), {"id": run.id})
        .scalar_one()
    )
    expected = {**original, "properties": {**original["properties"], "s3_bucket": "valsmith-dev-42-destination"}}
    assert persisted == expected
    assert persisted["priority"] == 3 and persisted["queue_pool_id"] == "saved-pool"
    request["completion_sha256"] = "c" * 64
    result = execute(relocation_session, request, "release")
    assert result.runs[0].hold_phase == "relocated_history_only"
    assert result.runs[0].hold_released_at is None
    with pytest.raises(LifecycleConflict):
        require_unheld(relocation_session, run.id)


@pytest.mark.parametrize(
    "status", [TaskStatus.PENDING, TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING]
)
def test_pending_or_deferred_tasks_block_location_change(relocation_session: Session, status: TaskStatus) -> None:
    run, original, request = seed(relocation_session)
    relocation_session.add(Task(org_id=run.org_id, benchmark=run.id, task_id="queued", status=status))
    relocation_session.commit()
    with pytest.raises(LifecycleConflict):
        execute(relocation_session, request, "prepare")
    relocation_session.rollback()
    persisted = relocation_session.get_one(Benchmark, run.id)
    assert persisted.arguments.properties is not None
    assert persisted.arguments.properties.s3_bucket == original["properties"]["s3_bucket"]
    assert relocation_session.get_one(RunLifecycle, run.id).released_at is None


@pytest.mark.parametrize("change", ["org", "resources", "arguments", "label", "active"])
def test_changed_run_scope_is_refused(relocation_session: Session, change: str) -> None:
    run, _, request = seed(relocation_session)
    execute(relocation_session, request, "prepare")
    if change == "org":
        request["org_id"] = str(uuid4())
    elif change == "resources":
        assert run.arguments.properties is not None
        run.arguments = run.arguments.model_copy(
            update={"properties": AWSResources("us-east-1", "changed-bucket", "runs", 7)}
        )
    elif change == "arguments":
        run.arguments = run.arguments.model_copy(update={"priority": 1})
    elif change == "label":
        run.label = "changed"
    else:
        run.status = BenchmarkStatus.IN_PROGRESS
    relocation_session.add(run)
    relocation_session.commit()
    with pytest.raises(LifecycleConflict):
        execute(relocation_session, request, "relocate")


def test_portable_unknown_reference_cannot_release(relocation_session: Session) -> None:
    run, _, request = seed(relocation_session, "portable")
    execute(relocation_session, request, "prepare")
    execute(relocation_session, request, "relocate")
    request["completion_sha256"] = "c" * 64
    with pytest.raises(LifecycleConflict):
        execute(relocation_session, request, "release")
    relocation_session.rollback()
    assert relocation_session.get_one(RunLifecycle, run.id).released_at is None


def test_completed_history_successor_has_no_release_gap_and_rejects_stale_plan(relocation_session: Session) -> None:

    run, _, request = seed(relocation_session)
    execute(relocation_session, request, "prepare")
    execute(relocation_session, request, "relocate")
    request["completion_sha256"] = "c" * 64
    execute(relocation_session, request, "release")
    original_identity = OperationIdentity.model_validate(request["plan"]["identity"])
    scope = RunScope(run_id=run.id, original_resources=request["plan"]["runs"][0]["destination_resources"])
    cached = relocation_session.get_one(RunLifecycle, run.id)
    predecessor = capture_predecessor(cached, original_identity, scope)
    relocation_session.commit()
    successor = original_identity.model_copy(update={"operation_id": uuid4(), "parent_plan_sha256": "d" * 64})
    with Session(relocation_session.get_bind(), expire_on_commit=False) as other:
        replacement = acquire_successor_hold(
            other, identity=successor, scope=scope, purpose="deletion", predecessor=predecessor
        )
        assert replacement.released_at is None
        with pytest.raises(LifecycleConflict):
            require_unheld(other, run.id)
        other.commit()
    with pytest.raises(LifecycleConflict):
        acquire_successor_hold(
            relocation_session,
            identity=original_identity.model_copy(update={"operation_id": uuid4()}),
            scope=scope,
            purpose="relocation",
            predecessor=predecessor,
        )
    relocation_session.rollback()
    retained = relocation_session.get_one(RunLifecycle, run.id)
    assert retained.purpose == "deletion" and retained.released_at is None
    assert json.loads(retained.identity_json)["operation_id"] == str(successor.operation_id)


def test_concurrent_successors_cannot_both_replace_completed_history(relocation_session: Session) -> None:

    run, _, request = seed(relocation_session)
    execute(relocation_session, request, "prepare")
    execute(relocation_session, request, "relocate")
    request["completion_sha256"] = "c" * 64
    execute(relocation_session, request, "release")
    identity = OperationIdentity.model_validate(request["plan"]["identity"])
    scope = RunScope(run_id=run.id, original_resources=request["plan"]["runs"][0]["destination_resources"])
    predecessor = capture_predecessor(relocation_session.get_one(RunLifecycle, run.id), identity, scope)
    relocation_session.commit()

    def replace(_index: int) -> bool:
        with Session(relocation_session.get_bind()) as session:
            try:
                acquire_successor_hold(
                    session,
                    identity=identity.model_copy(update={"operation_id": uuid4()}),
                    scope=scope,
                    purpose="relocation",
                    predecessor=predecessor,
                )
                session.commit()
                return True
            except LifecycleConflict:
                session.rollback()
                return False

    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(replace, range(2)))
    assert sorted(results) == [False, True]


@pytest.mark.parametrize("phase", ["held", "prepared", "relocated", "transferred_source_retired"])
def test_incomplete_or_retired_history_cannot_be_superseded(relocation_session: Session, phase: str) -> None:

    run, _, request = seed(relocation_session)
    execute(relocation_session, request, "prepare")
    record = relocation_session.get_one(RunLifecycle, run.id)
    record.phase = phase
    relocation_session.add(record)
    relocation_session.commit()
    with pytest.raises(LifecycleConflict):
        capture_predecessor(
            record,
            OperationIdentity.model_validate(request["plan"]["identity"]),
            RunScope.model_validate(request["plan"]["runs"][0]["scope"]),
        )


def test_cli_reads_real_postgresql_inventory_and_writes_private_nonce_bound_report(
    relocation_session: Session, tmp_path: Any
) -> None:

    run, _, request = seed(relocation_session)
    request["action"] = "inventory"
    request.pop("plan")
    request_path = tmp_path / "request.json"
    report_path = tmp_path / "response.json"
    request_path.write_text(json.dumps(request))
    script = Path(__file__).resolve().parents[4] / "scripts" / "relocate_run_storage.py"
    url = relocation_session.get_bind().engine.url.render_as_string(hide_password=False)
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--request",
            str(request_path),
            "--report",
            str(report_path),
            "--database-url-env",
            "RELOCATION_PRIVATE_DATABASE",
            "--expected-database-target",
            request["database_target"],
        ],
        env={**os.environ, "RELOCATION_PRIVATE_DATABASE": url},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    response = json.loads(report_path.read_text())
    assert response["nonce"] == request["nonce"]
    assert response["runs"][0]["label"] is None
    assert response["runs"][0]["resources"]["s3_bucket"] == "valsmith-dev-42-source"
    assert report_path.stat().st_mode & 0o777 == 0o600
    assert relocation_session.get(RunLifecycle, run.id) is None
    assert "install_cmd" not in report_path.read_text()


@pytest.mark.parametrize(
    "failure", ["sandbox", "destination", "account", "stale_host", "future_cutoff", "non_utc", "started_dispatch"]
)
def test_fresh_external_or_exit_failure_retains_hold_and_original_bucket(
    relocation_session: Session, failure: str
) -> None:

    run, original, request = seed(relocation_session)
    execute(relocation_session, request, "prepare")
    if failure == "stale_host":
        request["host_contract"]["observed_at"] = (datetime.now(UTC) - timedelta(minutes=16)).isoformat()
    elif failure == "future_cutoff":
        request["host_contract"]["acknowledgement_required_since"] = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    elif failure == "non_utc":
        request["host_contract"]["observed_at"] = datetime.now(UTC).astimezone(timezone(timedelta(hours=1))).isoformat()
    elif failure == "started_dispatch":
        release = ExecutorRelease(
            id="release", artifact_uri="s3://releases/test", artifact_digest="a" * 64, protocol_version="1"
        )
        relocation_session.add(release)
        relocation_session.flush()
        relocation_session.add(
            ExecutorDispatch(
                benchmark_id=run.id,
                executor_release_id=release.id,
                executor_artifact_uri=release.artifact_uri,
                executor_artifact_digest=release.artifact_digest,
                executor_protocol_version=release.protocol_version,
                kind=ExecutorDispatchKind.START,
                status=ExecutorDispatchStatus.FAILED,
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
            )
        )
        relocation_session.commit()

    class RefusingBoundary(EmptyBoundary):
        async def validate(self, *_arguments: object) -> None:
            if failure in {"destination", "account"}:
                raise LifecycleConflict("Exact owner tag or caller account differs")

        async def verify_absence(self, *_arguments: object) -> None:
            if failure == "sandbox":
                raise LifecycleConflict("Provider sandbox remains")

    request["action"] = "relocate"
    with pytest.raises(LifecycleConflict):
        asyncio.run(
            RelocationOperator(relocation_session, RefusingBoundary()).execute(TrackerRequest.model_validate(request))
        )
    relocation_session.rollback()
    assert relocation_session.get_one(Benchmark, run.id).arguments.properties == AWSResources(**original["properties"])
    assert relocation_session.get_one(RunLifecycle, run.id).released_at is None


def test_portable_release_is_idempotent_and_conflicting_completion_fails(relocation_session: Session) -> None:

    run, _, request = seed(relocation_session, "portable")

    class PortableBoundary(EmptyBoundary):
        async def execution_references(self, *_arguments: object) -> tuple[ExecutionReference, ...]:
            return (
                ExecutionReference(
                    pointer="/dataset",
                    value_sha256="a" * 64,
                    kind="retained_s3_object",
                    bucket="retained",
                    key="manifest",
                    version_id="v1",
                    sha256="b" * 64,
                ),
            )

    operator = RelocationOperator(relocation_session, PortableBoundary())
    for action in ("prepare", "relocate", "release", "release", "inspect"):
        request["action"] = action
        if action == "release":
            request["completion_sha256"] = "c" * 64
        response = asyncio.run(operator.execute(TrackerRequest.model_validate(request)))
    assert response.runs[0].hold_phase == "released"
    assert response.runs[0].hold_released_at is not None
    request["action"] = "release"
    request["completion_sha256"] = "d" * 64
    with pytest.raises(LifecycleConflict):
        asyncio.run(operator.execute(TrackerRequest.model_validate(request)))
    relocation_session.rollback()
    require_unheld(relocation_session, run.id)


@pytest.mark.parametrize("status", ["RUNNING", "QUEUED"])
def test_active_dispatch_refused_even_with_exit_receipt(relocation_session: Session, status: str) -> None:

    run, _, request = seed(relocation_session)
    release = ExecutorRelease(
        id="active-dispatch", artifact_uri="s3://releases/test", artifact_digest="a" * 64, protocol_version="1"
    )
    relocation_session.add(release)
    relocation_session.flush()
    dispatch = create_executor_dispatch(run.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    dispatch.status = ExecutorDispatchStatus(status)
    dispatch.started_at = datetime.now(UTC) if status == "RUNNING" else None
    dispatch.process_exited_at = datetime.now(UTC)
    relocation_session.add(dispatch)
    relocation_session.commit()
    with pytest.raises(LifecycleConflict):
        execute(relocation_session, request, "prepare")
    relocation_session.rollback()
    assert relocation_session.get_one(RunLifecycle, run.id).released_at is None


def test_completed_history_successor_must_include_current_run(relocation_session: Session) -> None:

    run, _, request = seed(relocation_session)
    execute(relocation_session, request, "prepare")
    execute(relocation_session, request, "relocate")
    request["completion_sha256"] = "c" * 64
    execute(relocation_session, request, "release")
    identity = OperationIdentity.model_validate(request["plan"]["identity"])
    scope = RunScope(run_id=run.id, original_resources=request["plan"]["runs"][0]["destination_resources"])
    predecessor = capture_predecessor(relocation_session.get_one(RunLifecycle, run.id), identity, scope)
    successor = identity.model_copy(update={"operation_id": uuid4(), "run_ids": (uuid4(),)})
    with pytest.raises(LifecycleConflict):
        acquire_successor_hold(
            relocation_session, identity=successor, scope=scope, purpose="deletion", predecessor=predecessor
        )


def test_legacy_external_drain_requires_exact_hold_and_evidence_file(
    relocation_session: Session, tmp_path: Any
) -> None:
    run, _, request = seed(relocation_session)
    release = ExecutorRelease(
        id="legacy", artifact_uri="s3://releases/test", artifact_digest="a" * 64, protocol_version="1"
    )
    relocation_session.add(release)
    relocation_session.flush()
    dispatch = create_executor_dispatch(run.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    dispatch.status = ExecutorDispatchStatus.FAILED
    dispatch.started_at = datetime.now(UTC) - timedelta(days=2)
    relocation_session.add(dispatch)
    relocation_session.commit()
    with pytest.raises(LifecycleConflict):
        execute(relocation_session, request, "prepare")
    relocation_session.rollback()
    hold = relocation_session.get_one(RunLifecycle, run.id)
    evidence = tmp_path / "host-drain.txt"
    evidence.write_bytes(b"fixture: hosts terminated and claims disabled")
    request["host_contract"]["legacy_dispatch_ids"] = [str(dispatch.id)]
    request["external_host_drains"] = [
        {
            "provenance": "externally_confirmed_host_drain",
            "identity": request["plan"]["identity"],
            "run_id": str(run.id),
            "hold_acquired_at": hold.acquired_at.replace(tzinfo=UTC).isoformat(),
            "dispatch_ids": [str(dispatch.id)],
            "host_inventory": ["host-1"],
            "deployed_host_contract": "stable-host-lifecycle-v1",
            "observed_at": datetime.now(UTC).isoformat(),
            "verifier": "test-operator",
            "evidence_sha256": hashlib.sha256(evidence.read_bytes()).hexdigest(),
            "confirmation": "all_inventory_hosts_terminated_and_old_claims_disabled",
        }
    ]
    request["external_evidence_files"] = [str(evidence)]
    response = execute(relocation_session, request, "prepare")
    assert response.runs[0].dispatches[0].evidence == "externally_confirmed_host_drain"
    assert response.runs[0].dispatches[0].process_exited_at is None
    evidence.write_bytes(b"changed evidence")
    with pytest.raises(LifecycleConflict):
        execute(relocation_session, request, "relocate")
    relocation_session.rollback()
    assert relocation_session.get_one(RunLifecycle, run.id).released_at is None


def test_prepare_resume_rejects_changed_provider_before_external_cleanup(relocation_session: Session) -> None:
    run, _, request = seed(relocation_session)
    execute(relocation_session, request, "prepare")
    run.arguments = run.arguments.model_copy(update={"sandbox_provider_secret_name": "unplanned-provider"})
    relocation_session.add(run)
    relocation_session.commit()
    cleaned: list[str] = []

    class RecordingBoundary(EmptyBoundary):
        async def cleanup_sandboxes(self, *_arguments: object) -> None:
            cleaned.append("provider mutation")

    with pytest.raises(LifecycleConflict):
        asyncio.run(
            RelocationOperator(relocation_session, RecordingBoundary()).execute(TrackerRequest.model_validate(request))
        )
    assert cleaned == []


def test_terminal_task_with_deferred_evaluation_blocks_relocation(relocation_session: Session) -> None:
    run, _, request = seed(relocation_session)
    relocation_session.add(
        Task(
            org_id=run.org_id,
            benchmark=run.id,
            task_id="deferred",
            status=TaskStatus.ERROR,
            eval_resume_state={"phase": "deferred"},
        )
    )
    relocation_session.commit()
    with pytest.raises(LifecycleConflict):
        execute(relocation_session, request, "prepare")
    relocation_session.rollback()
    assert relocation_session.get_one(RunLifecycle, run.id).released_at is None


@pytest.mark.parametrize("source_version", ["source-v1", "null"])
@pytest.mark.parametrize("source_tags", ["none", "organization"])
def test_legacy_shared_source_moves_through_real_provider_checks_without_touching_other_owner(
    relocation_session: Session, monkeypatch: pytest.MonkeyPatch, source_version: str, source_tags: str
) -> None:

    run, _, request = seed(relocation_session)
    source, destination = "legacy-shared-storage", "vs-dev-owner-42"
    run.arguments = run.arguments.model_copy(update={"properties": AWSResources("us-east-1", source, "runs", 7)})
    relocation_session.add(run)
    relocation_session.commit()
    planned = request["plan"]["runs"][0]
    planned["scope"]["original_resources"]["s3_bucket"] = source
    planned["destination_resources"]["s3_bucket"] = destination
    store = VersionStore(str(run.id))
    content = b"retained result"
    store.versions = {source: [(source_version, content)], destination: [("destination-v1", content)]}
    store.versioning[source] = {} if source_version == "null" else {"Status": "Suspended"}
    if source_tags == "organization":
        store.tags[source] = [{"Key": "valsmith:valkyrie-org-id", "Value": str(run.org_id)}]
    store.tags[destination] = [
        {"Key": key, "Value": value}
        for key, value in {
            "valsmith:environment": "dev",
            "valsmith:owner-account-id": "42",
            "valsmith:backup": "true",
            "valsmith:valkyrie-org-id": str(run.org_id),
        }.items()
    ]
    store.unrelated[source, f"benchmarks/{uuid4()}/other-owner.json"] = b"other owner result"
    unrelated = dict(store.unrelated)
    store.fence_statement = {
        "Sid": "ValSmithOwnerMigration" + request["plan"]["identity"]["operation_id"].replace("-", ""),
        "Effect": "Deny",
        "Principal": "*",
        "Action": ["s3:PutObject", "s3:DeleteObject"],
        "Resource": [f"arn:aws:s3:::{source}/benchmarks/{run.id}/*"],
    }
    clients = Mock()
    clients.credential_source = "managed"
    clients.with_region.return_value = clients
    clients.sts_client.return_value.get_caller_identity.return_value = {"Account": "123456789012"}
    clients.s3_client.return_value = store
    sandboxes = Mock()
    sandboxes.list_sandboxes.return_value = AsyncMock()
    sandboxes.close = AsyncMock()
    configuration = Mock()
    configuration.create_provider.return_value = sandboxes

    def sandbox_configuration(*_arguments: object) -> Mock:
        return configuration

    monkeypatch.setattr("tracker.run_purge.providers.fetch_sandbox_provider_config", sandbox_configuration)
    operator = RelocationOperator(relocation_session, RelocationAWSBoundary(clients))
    asyncio.run(operator.execute(TrackerRequest.model_validate(request)))
    content_digest = hashlib.sha256(content).hexdigest()
    request["copied_objects"] = [
        {
            "run_id": str(run.id),
            "key": store.key,
            "source_bucket": source,
            "source_version_id": source_version,
            "destination_bucket": destination,
            "destination_version_id": "destination-v1",
            "is_delete_marker": False,
            "source_sha256": content_digest,
            "destination_sha256": content_digest,
            "source_size": len(content),
            "destination_size": len(content),
            "is_current": True,
        }
    ]
    request["destination_versions"] = [
        {
            "run_id": str(run.id),
            "bucket": destination,
            "key": store.key,
            "version_id": "destination-v1",
            "is_delete_marker": False,
            "size": len(content),
            "sha256": content_digest,
            "is_current": True,
            "provenance": "copied",
        }
    ]
    request["action"] = "relocate"
    response = asyncio.run(operator.execute(TrackerRequest.model_validate(request)))
    assert response.runs[0].resources.s3_bucket == destination
    assert store.unrelated == unrelated
    assert store.versions[source] == [(source_version, content)]
    assert relocation_session.get_one(RunLifecycle, run.id).released_at is None


@pytest.mark.parametrize("action", ["inventory", "prepare", "inspect", "relocate", "release"])
@pytest.mark.parametrize("invalid_time", ["expired", "future", "cutoff", "non_utc"])
def test_invalid_host_observation_is_rejected_before_run_or_hold_lookup(
    relocation_session: Session, action: str, invalid_time: str
) -> None:
    run, _, request = seed(relocation_session)
    now = datetime.now(UTC)
    if invalid_time == "expired":
        request["host_contract"]["observed_at"] = (now - timedelta(minutes=16)).isoformat()
    elif invalid_time == "future":
        request["host_contract"]["observed_at"] = (now + timedelta(seconds=1)).isoformat()
    elif invalid_time == "cutoff":
        request["host_contract"]["acknowledgement_required_since"] = (now + timedelta(seconds=1)).isoformat()
    else:
        request["host_contract"]["observed_at"] = now.astimezone(timezone(timedelta(hours=1))).isoformat()

    relocation_session.delete(run)
    relocation_session.commit()
    with pytest.raises(LifecycleConflict, match="Host contract observation"):
        execute(relocation_session, request, action)
    assert relocation_session.get(RunLifecycle, run.id) is None


def test_stale_prepare_does_not_create_a_hold(relocation_session: Session) -> None:
    run, _, request = seed(relocation_session)
    request["host_contract"]["observed_at"] = (datetime.now(UTC) - timedelta(minutes=16)).isoformat()
    with pytest.raises(LifecycleConflict):
        execute(relocation_session, request, "prepare")
    relocation_session.rollback()
    assert relocation_session.get(RunLifecycle, run.id) is None


@pytest.mark.parametrize("age_minutes", [0, 15])
def test_prepare_accepts_inclusive_host_freshness_boundary_without_dispatches(
    relocation_session: Session, monkeypatch: pytest.MonkeyPatch, age_minutes: int
) -> None:
    run, _, request = seed(relocation_session)
    now = datetime.now(UTC)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return now

    monkeypatch.setattr("tracker.lifecycle_evidence.datetime", FixedDateTime)
    request["host_contract"]["observed_at"] = (now - timedelta(minutes=age_minutes)).isoformat()
    response = execute(relocation_session, request, "prepare")
    assert response.runs[0].dispatches == ()
    record = relocation_session.get(RunLifecycle, run.id)
    assert record is not None and record.phase == "prepared" and record.released_at is None
