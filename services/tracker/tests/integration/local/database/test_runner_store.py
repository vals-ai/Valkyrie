"""Executor runner dispatch-store integration against disposable PostgreSQL."""

import asyncio
import base64
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlmodel import Session, col, select

from tracker.executor.runner import (
    DispatchAuthority,
    PostgresExecutorDispatchStore,
    RenewalResult,
)
from tests.factories import make_benchmark, make_task
from tracker.executor.dispatch_payload import generate_payload_key, seal_payload
from tracker.database.models import (
    AgentContractRequest,
    BenchmarkStatus,
    ErrorResult,
    ExecutorDispatch,
    ExecutorDispatchPayload,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Org,
    Task,
    TaskStatus,
)
from tracker.executor.release_control import (
    create_executor_dispatch,
    pin_benchmark_to_release,
    promote_release,
    register_release,
)
from tracker.executor.dispatch_control import (
    admit_recovery_dispatch,
    admit_start_dispatch,
    reconcile_expired_dispatches,
)


@pytest.mark.asyncio
async def test_postgres_store_fences_claim_finish_and_terminalize_with_sibling(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    org = Org(id=uuid4(), name=f"executor-host-store-{uuid4()}")
    benchmark = make_benchmark(
        name="executor-host-store",
        org_id=org.id,
        contract=AgentContractRequest(name="store-agent", install_cmd="true", run_cmd="true"),
        status=BenchmarkStatus.IN_PROGRESS,
    )
    release = ExecutorRelease(
        id="executor-host-store-release",
        artifact_uri="s3://artifacts/executor-host-store.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
        created_at=datetime.now(UTC),
    )

    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    postgres_session.add(benchmark)
    postgres_session.flush()
    task = make_task(benchmark, "task-0", status=TaskStatus.IN_PROGRESS)
    newer_task = make_task(benchmark, "newer-task", status=TaskStatus.IN_PROGRESS)
    postgres_session.add_all([task, newer_task])
    postgres_session.flush()
    first_dispatch = create_executor_dispatch(
        benchmark.id,
        release,
        ExecutorDispatchKind.START,
        dispatch_id=uuid4(),
    )
    sibling_dispatch = create_executor_dispatch(
        benchmark.id,
        release,
        ExecutorDispatchKind.RETRY,
        dispatch_id=uuid4(),
    )
    benchmark.current_execution_release_id = release.id
    first_dispatch.assigned_task_ids = [task.task_id]
    sibling_dispatch.assigned_task_ids = [task.task_id, newer_task.task_id]
    postgres_session.add_all([first_dispatch, sibling_dispatch])
    expired_dispatch = create_executor_dispatch(
        benchmark.id,
        release,
        ExecutorDispatchKind.RESUME,
        dispatch_id=uuid4(),
    )
    expired_dispatch.claim_deadline_at = datetime.now(UTC) - timedelta(minutes=1)
    postgres_session.add(expired_dispatch)
    newer_task.started_at = sibling_dispatch.created_at + timedelta(seconds=1)
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"k" * 32).decode())
    for dispatch in (first_dispatch, sibling_dispatch, expired_dispatch):
        payload = {
            "start_benchmark_request_json": {
                "service_headers": {"authorization": "unique-persisted-dispatch-secret-marker"},
            },
            "benchmark_id_str": str(benchmark.id),
            "verified_task_ids": dispatch.assigned_task_ids or [],
            "telemetry_context_json": {"request_id": "store-request", "trace_headers": {}},
        }
        sealed = seal_payload(dispatch.id, payload, generate_payload_key(dispatch.id))
        postgres_session.add(
            ExecutorDispatchPayload(
                dispatch_id=dispatch.id,
                ciphertext=sealed.ciphertext,
                encrypted_data_key=sealed.encrypted_data_key,
                nonce=sealed.nonce,
                created_at=datetime.now(UTC),
            )
        )
    postgres_session.commit()
    persisted = postgres_session.execute(
        text(
            "SELECT ciphertext, encrypted_data_key, nonce FROM executor_dispatch_payload WHERE dispatch_id = :dispatch_id"
        ),
        {"dispatch_id": first_dispatch.id},
    ).one()
    assert all(b"unique-persisted-dispatch-secret-marker" not in bytes(column) for column in persisted)

    url = postgres_engine.url
    assert url.host is not None
    assert url.port is not None
    assert url.database is not None
    assert url.username is not None
    assert url.password is not None
    store = PostgresExecutorDispatchStore(
        host=url.host,
        port=str(url.port),
        dbname=url.database,
        user=url.username,
        password=url.password,
    )
    assert await store.claim(str(expired_dispatch.id)) is None
    assert reconcile_expired_dispatches(postgres_session) == 1
    postgres_session.commit()
    first_claim = await store.claim(str(first_dispatch.id))
    assert first_claim is not None
    assert first_claim.process_payload.arguments["telemetry_context_json"]["request_id"] == "store-request"
    assert (
        first_claim.process_payload.arguments["start_benchmark_request_json"]["service_headers"]["authorization"]
        == "unique-persisted-dispatch-secret-marker"
    )
    first_authority = first_claim.authority
    assert await store.claim(str(first_dispatch.id)) is None
    sibling_claim = await store.claim(str(sibling_dispatch.id))
    assert sibling_claim is not None
    sibling_authority = sibling_claim.authority
    assert await store.renew(first_authority) == RenewalResult(True, (True, False))
    assert await store.renew(sibling_authority) == RenewalResult(True, (True, False))
    postgres_session.expire_all()
    claimed_dispatch = postgres_session.get(type(first_dispatch), first_dispatch.id)
    assert claimed_dispatch is not None
    assert claimed_dispatch.started_at is not None
    assert claimed_dispatch.heartbeat_at is not None
    assert claimed_dispatch.lease_expires_at is not None
    assert await store.renew(sibling_authority) == RenewalResult(True, (True, False))

    assert await store.finish(first_authority)
    assert await store.renew(first_authority) == RenewalResult(False, (False, False))
    assert await store.renew(sibling_authority) == RenewalResult(True, (True, False))
    postgres_session.expire_all()
    persisted_benchmark = postgres_session.get(type(benchmark), benchmark.id)
    persisted_task = postgres_session.get(type(task), task.id)
    assert persisted_benchmark is not None
    assert persisted_benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert persisted_task is not None
    assert persisted_task.status == TaskStatus.IN_PROGRESS

    assert await store.terminalize(sibling_authority, [task.task_id, newer_task.task_id])
    postgres_session.expire_all()
    persisted_benchmark = postgres_session.get(type(benchmark), benchmark.id)
    persisted_task = postgres_session.get(type(task), task.id)
    persisted_newer_task = postgres_session.get(type(newer_task), newer_task.id)
    persisted_first_dispatch = postgres_session.get(type(first_dispatch), first_dispatch.id)
    persisted_sibling_dispatch = postgres_session.get(type(sibling_dispatch), sibling_dispatch.id)

    assert persisted_benchmark is not None
    assert persisted_benchmark.status == BenchmarkStatus.ERROR
    assert persisted_benchmark.error_message == "Executor host failed"
    assert persisted_task is not None
    assert persisted_task.status == TaskStatus.ERROR
    assert persisted_newer_task is not None
    assert persisted_newer_task.status == TaskStatus.IN_PROGRESS
    assert persisted_first_dispatch is not None
    assert persisted_first_dispatch.status == ExecutorDispatchStatus.FINISHED
    assert persisted_sibling_dispatch is not None
    assert persisted_sibling_dispatch.status == ExecutorDispatchStatus.FAILED
    assert persisted_sibling_dispatch.failure_reason == "EXECUTOR_FAILED"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "stopped", "expired", "locked", "mismatch", "expected"),
    [
        (ExecutorDispatchStatus.RUNNING, False, False, False, False, RenewalResult(True, (True, False))),
        (ExecutorDispatchStatus.RUNNING, False, False, True, False, RenewalResult(False, (True, False))),
        (ExecutorDispatchStatus.RUNNING, False, True, False, False, RenewalResult(False, (False, False))),
        (ExecutorDispatchStatus.FINISHED, False, False, False, False, RenewalResult(False, (False, False))),
        (ExecutorDispatchStatus.RUNNING, True, False, False, False, RenewalResult(True, (True, True))),
        (ExecutorDispatchStatus.FAILED, True, False, False, False, RenewalResult(False, (False, True))),
        (ExecutorDispatchStatus.RUNNING, True, False, False, True, RenewalResult(False, (False, False))),
    ],
)
async def test_renew_classifies_one_dispatch_without_batching(
    postgres_engine: Engine,
    postgres_session: Session,
    status: ExecutorDispatchStatus,
    stopped: bool,
    expired: bool,
    locked: bool,
    mismatch: bool,
    expected: RenewalResult,
) -> None:
    org = Org(id=uuid4(), name=f"lease-contract-{uuid4()}")
    benchmark = make_benchmark(
        org_id=org.id, status=BenchmarkStatus.STOPPED if stopped else BenchmarkStatus.IN_PROGRESS
    )
    release = ExecutorRelease(
        id=f"lease-contract-{uuid4()}",
        artifact_uri="s3://artifacts/lease.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
        created_at=datetime.now(UTC),
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    postgres_session.add(benchmark)
    postgres_session.flush()
    dispatch = create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    past = datetime.now(UTC) - timedelta(minutes=1)
    future = datetime.now(UTC) + timedelta(minutes=5)
    dispatch.status = status
    dispatch.heartbeat_at = past
    dispatch.lease_expires_at = past if expired else future
    postgres_session.add(dispatch)
    postgres_session.commit()

    url = postgres_engine.url
    assert url.host and url.port and url.database and url.username and url.password
    store = PostgresExecutorDispatchStore(
        host=url.host,
        port=str(url.port),
        dbname=url.database,
        user=url.username,
        password=url.password,
    )
    authority = DispatchAuthority(str(dispatch.id), str(benchmark.id) if not mismatch else str(org.id))
    connection = store._connect() if locked else None  # pyright: ignore[reportPrivateUsage]
    try:
        if connection is not None:
            with connection.cursor() as cursor:
                cursor.execute("SELECT id FROM executordispatch WHERE id = %s FOR UPDATE", (str(dispatch.id),))
        result = await asyncio.wait_for(store.renew(authority), timeout=2)
    finally:
        if connection is not None:
            connection.rollback()
            connection.close()

    assert result == expected
    postgres_session.expire_all()
    persisted = postgres_session.get(ExecutorDispatch, dispatch.id)
    assert persisted is not None and persisted.heartbeat_at is not None
    if expected.renewed:
        assert persisted.heartbeat_at > past.replace(tzinfo=None)
        assert persisted.lease_expires_at is not None
        assert persisted.lease_expires_at > persisted.heartbeat_at + timedelta(seconds=299)
    else:
        assert persisted.heartbeat_at == past.replace(tzinfo=None)
        assert persisted.lease_expires_at == (past if expired else future).replace(tzinfo=None)


@pytest.mark.asyncio
async def test_renew_commits_dispatch_lease_while_benchmark_table_is_locked(
    postgres_engine: Engine, postgres_session: Session
) -> None:
    org = Org(id=uuid4(), name=f"lease-lock-{uuid4()}")
    benchmark = make_benchmark(org_id=org.id, status=BenchmarkStatus.STOPPED)
    release = ExecutorRelease(
        id=f"lease-lock-release-{uuid4()}",
        artifact_uri="s3://artifacts/lease.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
        created_at=datetime.now(UTC),
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    postgres_session.add(benchmark)
    postgres_session.flush()
    dispatch = create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    dispatch.status = ExecutorDispatchStatus.RUNNING
    dispatch.heartbeat_at = datetime.now(UTC) - timedelta(minutes=1)
    dispatch.lease_expires_at = datetime.now(UTC) + timedelta(minutes=1)
    postgres_session.add(dispatch)
    postgres_session.commit()

    authority = DispatchAuthority(str(dispatch.id), str(benchmark.id))
    url = postgres_engine.url
    assert url.host and url.port and url.database and url.username and url.password
    store = PostgresExecutorDispatchStore(
        host=url.host,
        port=str(url.port),
        dbname=url.database,
        user=url.username,
        password=url.password,
    )
    with postgres_engine.connect() as reader:
        before = reader.execute(
            text("SELECT lease_expires_at FROM executordispatch WHERE id = :id"), {"id": dispatch.id}
        ).scalar_one()
    blocker = postgres_engine.raw_connection()
    try:
        with blocker.cursor() as cursor:
            cursor.execute("LOCK TABLE benchmark IN ACCESS EXCLUSIVE MODE")
        result = await asyncio.wait_for(store.renew(authority), timeout=4)
        assert result == RenewalResult(True, None)
        with postgres_engine.connect() as reader:
            after = reader.execute(
                text("SELECT lease_expires_at FROM executordispatch WHERE id = :id"), {"id": dispatch.id}
            ).scalar_one()
        assert after > before
    finally:
        blocker.rollback()
        blocker.close()

    classified = await store.renew(authority)
    assert classified == RenewalResult(True, (True, True))


@pytest.mark.parametrize("dispatch_count", [1, 2])
@pytest.mark.parametrize("running", [False, True])
def test_expiry_cleans_all_admitted_assignments(postgres_session: Session, dispatch_count: int, running: bool) -> None:
    """Verify expired dispatches leave no assigned tasks active after recovery.

    Test cases:
    - Fresh START tasks use their normal model timestamp.
    - Queued and running dispatches recover alone or with an expired sibling.
    """
    org = Org(id=uuid4(), name=f"review-{uuid4()}")
    benchmark = make_benchmark(org_id=org.id)
    release = ExecutorRelease(
        id=f"review-{uuid4()}",
        artifact_uri="s3://artifacts/review.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
    )
    postgres_session.add(org)
    postgres_session.flush()
    register_release(postgres_session, release)
    promote_release(postgres_session, release.id)
    postgres_session.commit()
    tasks: list[Task] = []
    dispatches: list[ExecutorDispatch] = []
    for index in range(dispatch_count):
        task = Task(org_id=org.id, benchmark=benchmark.id, task_id=f"task-{index}")
        postgres_session.add(task)
        if index == 0:
            dispatch = admit_start_dispatch(
                postgres_session, benchmark=benchmark, dispatch_id=uuid4(), task_ids=[task.task_id]
            )
        else:
            dispatch = admit_recovery_dispatch(
                postgres_session,
                benchmark=benchmark,
                pre_action_status=benchmark.status,
                dispatch_id=uuid4(),
                kind=ExecutorDispatchKind.RESUME,
                task_ids=[task.task_id],
            )
        assert task.started_at is not None
        assert task.started_at <= dispatch.created_at
        dispatch.claim_deadline_at = datetime.now(UTC) - timedelta(minutes=1)
        if running:
            dispatch.status = ExecutorDispatchStatus.RUNNING
            dispatch.started_at = datetime.now(UTC) - timedelta(minutes=6)
            dispatch.lease_expires_at = datetime.now(UTC) - timedelta(minutes=1)
            task.status = TaskStatus.IN_PROGRESS
        tasks.append(task)
        dispatches.append(dispatch)
    postgres_session.commit()

    assert reconcile_expired_dispatches(postgres_session) == dispatch_count
    postgres_session.commit()
    for row in [benchmark, *tasks, *dispatches]:
        postgres_session.refresh(row)

    assert benchmark.status == BenchmarkStatus.ERROR
    assert all(row.status == ExecutorDispatchStatus.FAILED for row in dispatches)
    assert [row.status for row in tasks] == [TaskStatus.ERROR] * dispatch_count
    errors = postgres_session.exec(
        select(ErrorResult).where(col(ErrorResult.task).in_([row.id for row in tasks]))
    ).all()
    assert len(errors) == dispatch_count


def _sealed_case(
    session: Session, engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> tuple[PostgresExecutorDispatchStore, ExecutorDispatch]:
    monkeypatch.setenv("EXECUTOR_LAUNCHER", "local")
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"k" * 32).decode())
    org = Org(id=uuid4(), name=f"runner-claim-{uuid4()}")
    benchmark = make_benchmark(
        name="runner-claim",
        org_id=org.id,
        contract=AgentContractRequest(name="claim-agent", install_cmd="true", run_cmd="true"),
        status=BenchmarkStatus.IN_PROGRESS,
    )
    release = ExecutorRelease(
        id=f"runner-claim-{uuid4()}",
        artifact_uri="s3://artifacts/runner.pex",
        artifact_digest="a" * 64,
        protocol_version="1",
        readiness_verified=True,
        created_at=datetime.now(UTC),
    )
    session.add(org)
    session.flush()
    register_release(session, release)
    pin_benchmark_to_release(benchmark, release)
    benchmark.current_execution_release_id = release.id
    session.add(benchmark)
    session.flush()
    dispatch = create_executor_dispatch(benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4())
    dispatch.assigned_task_ids = ["task-0"]
    session.add(dispatch)
    sealed = seal_payload(
        dispatch.id,
        {
            "start_benchmark_request_json": {"request_id": "admitted-request"},
            "benchmark_id_str": str(benchmark.id),
            "verified_task_ids": ["task-0"],
            "telemetry_context_json": {
                "request_id": "admitted-request",
                "trace_headers": {"traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"},
            },
        },
        generate_payload_key(dispatch.id),
    )
    session.add(
        ExecutorDispatchPayload(
            dispatch_id=dispatch.id,
            ciphertext=sealed.ciphertext,
            encrypted_data_key=sealed.encrypted_data_key,
            nonce=sealed.nonce,
            created_at=datetime.now(UTC),
        )
    )
    session.commit()
    url = engine.url
    assert url.host and url.port and url.database and url.username and url.password
    return PostgresExecutorDispatchStore(
        host=url.host,
        port=str(url.port),
        dbname=url.database,
        user=url.username,
        password=url.password,
    ), dispatch


@pytest.mark.asyncio
async def test_concurrent_claimants_have_one_payload_and_owner(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, dispatch = _sealed_case(postgres_session, postgres_engine, monkeypatch)
    claims = await asyncio.gather(store.claim(str(dispatch.id)), store.claim(str(dispatch.id)))
    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    assert winners[0].process_payload.arguments["start_benchmark_request_json"]["request_id"] == "admitted-request"
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is None
    assert postgres_session.get(ExecutorDispatch, dispatch.id).status == ExecutorDispatchStatus.RUNNING


@pytest.mark.asyncio
async def test_decrypt_failure_rolls_back_claim_and_preserves_payload(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, dispatch = _sealed_case(postgres_session, postgres_engine, monkeypatch)
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"z" * 32).decode())
    with pytest.raises(Exception):
        await store.claim(str(dispatch.id))
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatch, dispatch.id).status == ExecutorDispatchStatus.QUEUED
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["expired", "superseded", "nonqueued"])
async def test_nonclaimable_dispatch_never_reads_or_deletes_payload(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
) -> None:
    store, dispatch = _sealed_case(postgres_session, postgres_engine, monkeypatch)
    if state == "expired":
        dispatch.claim_deadline_at = datetime.now(UTC) - timedelta(seconds=1)
    elif state == "superseded":
        dispatch.status = ExecutorDispatchStatus.FAILED
        dispatch.failure_reason = "SUPERSEDED"
    else:
        dispatch.status = ExecutorDispatchStatus.FINISHED
    postgres_session.add(dispatch)
    postgres_session.commit()
    monkeypatch.setenv("EXECUTOR_PAYLOAD_LOCAL_KEY", base64.b64encode(b"z" * 32).decode())
    assert await store.claim(str(dispatch.id)) is None
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatchPayload, dispatch.id) is not None


@pytest.mark.asyncio
async def test_missing_queued_payload_is_error_without_claim_commit(
    postgres_engine: Engine,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store, dispatch = _sealed_case(postgres_session, postgres_engine, monkeypatch)
    row = postgres_session.get(ExecutorDispatchPayload, dispatch.id)
    assert row is not None
    postgres_session.delete(row)
    postgres_session.commit()
    with pytest.raises(ValueError, match="no sealed payload"):
        await store.claim(str(dispatch.id))
    postgres_session.expire_all()
    assert postgres_session.get(ExecutorDispatch, dispatch.id).status == ExecutorDispatchStatus.QUEUED
