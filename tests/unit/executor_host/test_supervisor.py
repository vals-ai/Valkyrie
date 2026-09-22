"""Tests for ExecutorHost dispatch supervision.

Run: uv run pytest tests/unit/executor_host/test_supervisor.py
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from json import JSONDecodeError
import logging
import sys
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock

import pytest

import services.executor_host.observability as host_observability
import services.executor_host.supervisor as supervisor_module
from services.executor_host.supervisor import (  # pyright: ignore[reportMissingImports]
    ArtifactDispatch,
    DispatchAuthority,
    DispatchAuthorityLostError,
    DeleteAfterAckRedisStreamBroker,
    ExecutorProcessPayload,
    ExecutorSupervisor,
    PostgresExecutorDispatchStore,
    run_executor_dispatch,
    verify_file_digest,
)
from executor_protocol import ExecutorTelemetryContext, validate_executor_artifact_uri


@pytest.fixture(autouse=True)
def reset_task_protection_state() -> None:
    supervisor_module._active_execution_count = 0  # pyright: ignore[reportPrivateUsage]
    supervisor_module._protection_waiter_count = 0  # pyright: ignore[reportPrivateUsage]
    supervisor_module._protection_refresh_task = None  # pyright: ignore[reportPrivateUsage]
    supervisor_module._protection_admission_open = asyncio.Event()  # pyright: ignore[reportPrivateUsage]
    supervisor_module._confirmed_protection_expiration = None  # pyright: ignore[reportPrivateUsage]
    supervisor_module._execution_lock = asyncio.Lock()  # pyright: ignore[reportPrivateUsage]


class FakeDispatchStore:
    def __init__(
        self,
        *,
        claim_result: bool = True,
        authority_results: list[bool] | None = None,
        finish_result: bool = True,
    ) -> None:
        self.claim_result = claim_result
        self.authority_results = authority_results or [True]
        self.finish_result = finish_result
        self.claimed: list[tuple[str, str, ArtifactDispatch]] = []
        self.authority_checks: list[DispatchAuthority] = []
        self.terminalized: list[DispatchAuthority] = []
        self.finished: list[DispatchAuthority] = []
        self.heartbeats: list[DispatchAuthority] = []
        self.authority: DispatchAuthority | None = None

    async def claim(
        self,
        dispatch_id: str,
        benchmark_id: str,
        dispatch: ArtifactDispatch,
    ) -> DispatchAuthority | None:
        self.claimed.append((dispatch_id, benchmark_id, dispatch))
        if not self.claim_result or self.authority is not None:
            return None
        self.authority = DispatchAuthority(
            dispatch_id=dispatch_id,
            benchmark_id=benchmark_id,
        )
        return self.authority

    async def is_current(self, authority: DispatchAuthority) -> bool:
        self.authority_checks.append(authority)
        if len(self.authority_results) == 1:
            return self.authority_results[0]
        return self.authority_results.pop(0)

    async def heartbeat(self, authority: DispatchAuthority) -> bool:
        self.heartbeats.append(authority)
        return True

    async def terminalize(self, authority: DispatchAuthority, task_ids: list[str]) -> bool:
        _ = task_ids
        self.terminalized.append(authority)
        return True

    async def finish(self, authority: DispatchAuthority) -> bool:
        self.finished.append(authority)
        return self.finish_result


class FakeS3Client:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.calls: list[tuple[str, str, str]] = []

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        self.calls.append((bucket, key, filename))
        Path(filename).write_bytes(self.content)


class RecordingCursor:
    def __init__(self, row: tuple[object, ...] | None | list[tuple[object, ...] | None]) -> None:
        self.rows = row if isinstance(row, list) else [row]
        self.statements: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self) -> RecordingCursor:
        return self

    def __exit__(self, *args: object) -> None:
        pass

    def execute(self, statement: str, parameters: tuple[object, ...]) -> None:
        self.statements.append((" ".join(statement.split()), parameters))

    def fetchone(self) -> tuple[object, ...] | None:
        if len(self.rows) == 1:
            return self.rows[0]
        return self.rows.pop(0)


class RecordingConnection:
    def __init__(self, cursor: RecordingCursor) -> None:
        self.recording_cursor = cursor

    def __enter__(self) -> RecordingConnection:
        return self

    def __exit__(self, *args: object) -> None:
        pass

    def cursor(self) -> RecordingCursor:
        return self.recording_cursor


class MockRedis:
    def __init__(
        self,
        *,
        connection_pool: object,
        commands: list[tuple[object, ...]],
        eval_result: int | Exception,
    ) -> None:
        self.connection_pool = connection_pool
        self.commands = commands
        self.eval_result = eval_result

    async def __aenter__(self) -> MockRedis:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def eval(self, script: str, numkeys: int, *keys_and_args: str) -> int:
        self.commands.append(("eval", script, numkeys, *keys_and_args))
        if isinstance(self.eval_result, Exception):
            raise self.eval_result

        return self.eval_result


def _dispatch(*, digest: str) -> ArtifactDispatch:
    return ArtifactDispatch.from_payload(
        {
            "executor_release_id": "release-v2",
            "executor_artifact_uri": "s3://artifacts/executors/v2.pex",
            "executor_artifact_digest": digest,
            "executor_protocol_version": "1",
        }
    )


def _process_payload(
    request: dict[str, object] | None = None,
    *,
    benchmark_id: str = "benchmark-1",
    task_ids: list[str] | None = None,
) -> ExecutorProcessPayload:
    return ExecutorProcessPayload.from_payload(
        {
            "start_benchmark_request_json": request or {},
            "benchmark_id_str": benchmark_id,
            "verified_task_ids": task_ids or [],
        },
        telemetry_context={"request_id": "", "trace_headers": {}},
    )


def test_managed_process_payload_includes_child_telemetry_context() -> None:
    context: dict[str, object] = {
        "benchmark_id": "benchmark-1",
        "verified_task_ids": ["task-1"],
        "start_benchmark_request": {},
    }
    telemetry_context: ExecutorTelemetryContext = {
        "request_id": "request-1",
        "trace_headers": {"sentry-trace": "trace-header"},
    }

    payload = ExecutorProcessPayload.from_payload(
        {"execution_context_json": context},
        telemetry_context=telemetry_context,
    )

    assert payload.benchmark_id == "benchmark-1"
    assert payload.verified_task_ids == ["task-1"]
    assert payload.arguments == {
        "execution_context_json": context,
        "telemetry_context_json": telemetry_context,
    }


def test_process_payload_rejects_mixed_execution_shapes() -> None:
    with pytest.raises(ValueError, match="mixes access-key and managed"):
        ExecutorProcessPayload.from_payload(
            {
                "execution_context_json": {
                    "benchmark_id": "benchmark-1",
                    "verified_task_ids": [],
                },
                "start_benchmark_request_json": {},
            },
            telemetry_context={"request_id": "", "trace_headers": {}},
        )


def _supervisor(
    tmp_path: Path,
    *,
    content: bytes,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> ExecutorSupervisor:
    return ExecutorSupervisor(
        cache_dir=tmp_path,
        s3_client=FakeS3Client(content),
        python_executable=sys.executable,
        artifact_bucket="artifacts",
        artifact_prefix="executors",
        sleep=sleep,
    )


def test_dispatch_rejects_missing_or_invalid_identity() -> None:
    with pytest.raises(ValueError, match="executor_artifact_digest"):
        ArtifactDispatch.from_payload(
            {
                "executor_release_id": "release-v2",
                "executor_artifact_uri": "s3://artifacts/v2.pex",
                "executor_protocol_version": "1",
            }
        )

    with pytest.raises(ValueError, match="64-character"):
        _dispatch(digest="not-a-digest")


def test_verify_file_digest_rejects_mismatch(tmp_path: Path) -> None:
    artifact = tmp_path / "executor.pex"
    artifact.write_bytes(b"executor")
    digest = hashlib.sha256(b"executor").hexdigest()

    verify_file_digest(artifact, digest)
    with pytest.raises(ValueError, match="digest mismatch"):
        verify_file_digest(artifact, "0" * 64)


@pytest.mark.asyncio
async def test_prepare_artifact_downloads_and_verifies_by_digest(tmp_path: Path) -> None:
    content = b"immutable executor"
    digest = hashlib.sha256(content).hexdigest()
    client = FakeS3Client(content)
    supervisor = ExecutorSupervisor(
        cache_dir=tmp_path, s3_client=client, artifact_bucket="artifacts", artifact_prefix="executors"
    )
    dispatch = _dispatch(digest=digest)

    artifact_path = await supervisor.prepare_artifact(dispatch)

    assert artifact_path == tmp_path / f"{digest}.pex"
    assert artifact_path.read_bytes() == content
    assert artifact_path.stat().st_mode & 0o111
    assert len(client.calls) == 1
    bucket, key, temporary_name = client.calls[0]
    assert (bucket, key) == ("artifacts", "executors/v2.pex")
    assert Path(temporary_name).parent == tmp_path
    assert Path(temporary_name).suffix == ".tmp"
    assert Path(temporary_name).name != f"{digest}.tmp"


def test_validate_artifact_uri_requires_configured_bucket_and_prefix() -> None:
    assert validate_executor_artifact_uri(
        "s3://artifacts/executors/v2.pex",
        "artifacts",
        "executors",
    ) == ("artifacts", "executors/v2.pex")

    with pytest.raises(ValueError, match="identify an S3 object"):
        validate_executor_artifact_uri("https://artifacts/executors/v2.pex", "artifacts", "executors")
    with pytest.raises(ValueError, match="identify an S3 object"):
        validate_executor_artifact_uri("s3://artifacts/", "artifacts", "executors")
    with pytest.raises(ValueError, match="configured S3 bucket and prefix"):
        validate_executor_artifact_uri("s3://other/executors/v2.pex", "artifacts", "executors")
    with pytest.raises(ValueError, match="configured S3 bucket and prefix"):
        validate_executor_artifact_uri("s3://artifacts/other/v2.pex", "artifacts", "executors")


@pytest.mark.asyncio
async def test_postgres_claim_is_status_fenced_and_returns_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = RecordingCursor(("dispatch-1",))
    store = PostgresExecutorDispatchStore(
        host="db",
        port="5432",
        dbname="tracker",
        user="tracker",
        password="secret",
    )
    monkeypatch.setattr(store, "_connect", lambda: RecordingConnection(cursor))

    authority = await store.claim(
        "dispatch-1",
        "benchmark-1",
        _dispatch(digest="0" * 64),
    )

    assert authority == DispatchAuthority(
        dispatch_id="dispatch-1",
        benchmark_id="benchmark-1",
    )
    statement, parameters = cursor.statements[0]
    assert "UPDATE executordispatch AS dispatch" in statement
    assert "FROM benchmark" in statement
    assert "benchmark.status = 'IN_PROGRESS'" in statement
    assert "dispatch.status = 'QUEUED'" in statement
    assert "dispatch.claim_deadline_at > CURRENT_TIMESTAMP" in statement
    assert "SET status = 'RUNNING'" in statement
    assert "started_at = CURRENT_TIMESTAMP" in statement
    assert parameters[1:3] == ("dispatch-1", "benchmark-1")


@pytest.mark.asyncio
async def test_postgres_authority_and_completion_are_fenced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = RecordingCursor([(True,), ("dispatch-1",), ("FINISHED",), ("dispatch-1",)])
    store = PostgresExecutorDispatchStore(
        host="db",
        port="5432",
        dbname="tracker",
        user="tracker",
        password="secret",
    )
    monkeypatch.setattr(store, "_connect", lambda: RecordingConnection(cursor))
    authority = DispatchAuthority(
        dispatch_id="dispatch-1",
        benchmark_id="benchmark-1",
    )

    assert await store.is_current(authority)
    assert await store.heartbeat(authority)
    assert await store.finish(authority)

    authority_statement, authority_parameters = cursor.statements[0]
    heartbeat_statement, heartbeat_parameters = cursor.statements[1]
    finish_lock_statement, finish_lock_parameters = cursor.statements[2]
    finish_statement, finish_parameters = cursor.statements[3]
    assert "dispatch.status = 'RUNNING'" in authority_statement
    assert "benchmark.status != 'STOPPED'" in authority_statement
    assert "dispatch.lease_expires_at > CURRENT_TIMESTAMP" in authority_statement
    assert authority_parameters == ("dispatch-1",)
    assert "lease_expires_at > CURRENT_TIMESTAMP" in heartbeat_statement
    assert heartbeat_parameters == (
        supervisor_module.DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS,
        "dispatch-1",
        "benchmark-1",
    )
    assert "FOR UPDATE" in finish_lock_statement
    assert finish_lock_parameters == ("benchmark-1",)
    assert "SET status = 'FINISHED'" in finish_statement
    assert "lease_expires_at > CURRENT_TIMESTAMP" in finish_statement
    assert finish_parameters == ("dispatch-1", "benchmark-1")


@pytest.mark.asyncio
async def test_postgres_finish_errors_orphaned_in_progress_benchmark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = RecordingCursor([("IN_PROGRESS",), ("dispatch-1",), (False,)])
    store = PostgresExecutorDispatchStore(
        host="db",
        port="5432",
        dbname="tracker",
        user="tracker",
        password="secret",
    )
    monkeypatch.setattr(store, "_connect", lambda: RecordingConnection(cursor))
    authority = DispatchAuthority(dispatch_id="dispatch-1", benchmark_id="benchmark-1")

    assert await store.finish(authority)

    assert "FOR UPDATE" in cursor.statements[0][0]
    assert "SET status = 'FINISHED'" in cursor.statements[1][0]
    assert "SELECT EXISTS" in cursor.statements[2][0]
    assert "SET status = 'ERROR'" in cursor.statements[3][0]


@pytest.mark.asyncio
async def test_postgres_terminalize_marks_current_run_and_runnable_tasks_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = RecordingCursor([("IN_PROGRESS",), ("dispatch-1",), (False,)])
    store = PostgresExecutorDispatchStore(
        host="db",
        port="5432",
        dbname="tracker",
        user="tracker",
        password="secret",
    )
    monkeypatch.setattr(store, "_connect", lambda: RecordingConnection(cursor))
    authority = DispatchAuthority(
        dispatch_id="dispatch-1",
        benchmark_id="benchmark-1",
    )

    assert await store.terminalize(authority, ["task-1"])

    lock_statement = cursor.statements[0][0]
    dispatch_statement = cursor.statements[1][0]
    task_statement, task_parameters = cursor.statements[2]
    benchmark_statement = cursor.statements[4][0]
    assert "FOR UPDATE" in lock_statement
    assert "SET status = 'FAILED'" in dispatch_statement
    assert "lease_expires_at > CURRENT_TIMESTAMP" in dispatch_statement
    assert "task_id = ANY(%s)" in task_statement
    assert "started_at <= ( SELECT created_at FROM executordispatch" in task_statement
    assert "status IN ('PENDING', 'BUILDING', 'IN_PROGRESS', 'EVALUATING')" in task_statement
    assert task_parameters == ("benchmark-1", ["task-1"], "dispatch-1")
    assert "SET status = 'ERROR'" in benchmark_statement


@pytest.mark.asyncio
async def test_postgres_terminalize_keeps_benchmark_active_for_coexisting_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = RecordingCursor([("IN_PROGRESS",), ("dispatch-1",), (True,)])
    store = PostgresExecutorDispatchStore(
        host="db",
        port="5432",
        dbname="tracker",
        user="tracker",
        password="secret",
    )
    monkeypatch.setattr(store, "_connect", lambda: RecordingConnection(cursor))
    authority = DispatchAuthority(
        dispatch_id="dispatch-1",
        benchmark_id="benchmark-1",
    )

    assert await store.terminalize(authority, ["retry-task"])

    assert len(cursor.statements) == 4
    assert cursor.statements[2][1] == ("benchmark-1", ["retry-task"], "dispatch-1")
    assert "SELECT EXISTS" in cursor.statements[3][0]


@pytest.mark.asyncio
async def test_run_forwards_dispatch_authority_to_executor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    script = b"""import json, os, sys\nfrom pathlib import Path\npayload = json.loads(Path(sys.argv[1]).read_text())\nresult = {\"payload\": payload, \"sentry_release\": os.environ.get(\"SENTRY_RELEASE\"), \"pool_size\": os.environ.get(\"DATABASE_POOL_SIZE\"), \"max_overflow\": os.environ.get(\"DATABASE_MAX_OVERFLOW\")}\nPath(os.environ[\"EXECUTOR_TEST_MARKER\"]).write_text(json.dumps(result))\n"""
    digest = hashlib.sha256(script).hexdigest()
    marker = tmp_path / "marker.json"
    monkeypatch.setenv("EXECUTOR_TEST_MARKER", str(marker))
    monkeypatch.setenv("DATABASE_POOL_SIZE", "5")
    monkeypatch.setenv("DATABASE_MAX_OVERFLOW", "2")
    store = FakeDispatchStore()

    with caplog.at_level(logging.INFO, logger=supervisor_module.logger.name):
        await run_executor_dispatch(
            _supervisor(tmp_path, content=script),
            store,
            executor_dispatch_id="dispatch-1",
            dispatch=_dispatch(digest=digest),
            process_payload=_process_payload({"benchmark_name": "swebench"}, task_ids=["task-1"]),
        )

    try:
        result = json.loads(marker.read_text())
    except (OSError, JSONDecodeError) as error:
        raise AssertionError(f"executor marker was not valid JSON: {error}") from error
    payload = result["payload"]
    assert payload["benchmark_id_str"] == "benchmark-1"
    assert payload["verified_task_ids"] == ["task-1"]
    assert payload["executor_dispatch_id"] == "dispatch-1"
    assert payload["telemetry_context_json"] == {"request_id": "", "trace_headers": {}}
    assert result["sentry_release"] == "release-v2"
    assert result["pool_size"] == "5"
    assert result["max_overflow"] == "2"
    assert store.authority_checks
    assert store.finished == [store.authority]
    assert (
        f"Launching benchmark benchmark-1 dispatch_id=dispatch-1 release=release-v2 digest={digest} protocol=1"
    ) in caplog.messages


@pytest.mark.asyncio
async def test_run_renews_heartbeat_and_stops_it_after_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    heartbeat_seen = asyncio.Event()

    class FakeExecutorSupervisor:
        async def prepare_artifact(self, _dispatch: ArtifactDispatch) -> Path:
            return tmp_path / "executor.pex"

        async def run(self, *_args: object, **_kwargs: object) -> None:
            await heartbeat_seen.wait()

    store = FakeDispatchStore()
    original_heartbeat = store.heartbeat

    async def record_heartbeat(authority: DispatchAuthority) -> bool:
        result = await original_heartbeat(authority)
        heartbeat_seen.set()
        return result

    monkeypatch.setattr(store, "heartbeat", record_heartbeat)

    await run_executor_dispatch(
        FakeExecutorSupervisor(),  # type: ignore[arg-type]
        store,
        executor_dispatch_id="dispatch-1",
        dispatch=_dispatch(digest="0" * 64),
        process_payload=_process_payload(),
        heartbeat_interval_seconds=0,
    )

    heartbeat_count_after_cleanup = len(store.heartbeats)
    assert heartbeat_count_after_cleanup >= 1
    await asyncio.sleep(0)
    assert len(store.heartbeats) == heartbeat_count_after_cleanup


@pytest.mark.asyncio
async def test_heartbeat_lease_expires_from_last_confirmed_renewal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0
    sleep_delays: list[float] = []
    store = FakeDispatchStore()
    authority = DispatchAuthority(dispatch_id="dispatch-1", benchmark_id="benchmark-1")
    lease = supervisor_module._DispatchLease(  # pyright: ignore[reportPrivateUsage]
        last_confirmed_renewal_at=now,
        lost=asyncio.Event(),
    )

    async def advance_time(delay: float) -> None:
        nonlocal now
        sleep_delays.append(delay)
        now += delay

    async def heartbeat(current_authority: DispatchAuthority) -> bool:
        nonlocal now
        store.heartbeats.append(current_authority)
        if len(store.heartbeats) == 1:
            return True
        now = 1 + supervisor_module.DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS
        raise supervisor_module.psycopg2.OperationalError("temporary")

    monkeypatch.setattr(supervisor_module, "_monotonic_time", lambda: now)
    monkeypatch.setattr(supervisor_module.asyncio, "sleep", advance_time)
    monkeypatch.setattr(store, "heartbeat", heartbeat)

    await supervisor_module._heartbeat_loop(  # pyright: ignore[reportPrivateUsage]
        store,
        authority,
        lease,
        interval_seconds=1,
    )

    assert lease.lost.is_set()
    assert store.heartbeats == [authority, authority]
    assert sleep_delays == [1, 1]
    assert now == 1 + supervisor_module.DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS


@pytest.mark.asyncio
async def test_heartbeat_returning_after_deadline_cannot_renew_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0
    heartbeat_started_at: float | None = None
    store = FakeDispatchStore()
    authority = DispatchAuthority(dispatch_id="dispatch-1", benchmark_id="benchmark-1")
    lease = supervisor_module._DispatchLease(  # pyright: ignore[reportPrivateUsage]
        last_confirmed_renewal_at=now,
        lost=asyncio.Event(),
    )

    async def advance_time(delay: float) -> None:
        nonlocal now
        now += delay

    async def heartbeat(current_authority: DispatchAuthority) -> bool:
        nonlocal heartbeat_started_at, now
        heartbeat_started_at = now
        store.heartbeats.append(current_authority)
        now = supervisor_module.DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS + 0.1
        return True

    monkeypatch.setattr(supervisor_module, "_monotonic_time", lambda: now)
    monkeypatch.setattr(supervisor_module.asyncio, "sleep", advance_time)
    monkeypatch.setattr(store, "heartbeat", heartbeat)

    await supervisor_module._heartbeat_loop(  # pyright: ignore[reportPrivateUsage]
        store,
        authority,
        lease,
        interval_seconds=1,
    )

    assert lease.lost.is_set()
    assert heartbeat_started_at == 1
    assert heartbeat_started_at < supervisor_module.DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS
    assert now > supervisor_module.DEFAULT_EXECUTOR_DISPATCH_LEASE_SECONDS
    assert lease.last_confirmed_renewal_at == 0
    assert store.heartbeats == [authority]


@pytest.mark.asyncio
async def test_non_claimable_dispatch_does_not_launch(tmp_path: Path) -> None:
    store = FakeDispatchStore(claim_result=False)
    artifact = b"unused"
    s3_client = FakeS3Client(artifact)
    supervisor = ExecutorSupervisor(
        cache_dir=tmp_path,
        s3_client=s3_client,
        python_executable=sys.executable,
        artifact_bucket="artifacts",
        artifact_prefix="executors",
    )

    await run_executor_dispatch(
        supervisor,
        store,
        executor_dispatch_id="dispatch-1",
        dispatch=_dispatch(digest=hashlib.sha256(artifact).hexdigest()),
        process_payload=_process_payload(),
    )

    assert len(store.claimed) == 1
    assert store.authority is None
    assert store.finished == []
    assert s3_client.calls == []


@pytest.mark.asyncio
async def test_duplicate_dispatch_claim_does_not_launch_again(tmp_path: Path) -> None:
    script = b"print('ok')"
    digest = hashlib.sha256(script).hexdigest()
    store = FakeDispatchStore()
    supervisor = _supervisor(tmp_path, content=script)

    await run_executor_dispatch(
        supervisor,
        store,
        executor_dispatch_id="dispatch-1",
        dispatch=_dispatch(digest=digest),
        process_payload=_process_payload(),
    )
    await run_executor_dispatch(
        supervisor,
        store,
        executor_dispatch_id="dispatch-1",
        dispatch=_dispatch(digest=digest),
        process_payload=_process_payload(),
    )

    assert len(store.claimed) == 2
    assert len(store.finished) == 1


@pytest.mark.asyncio
async def test_artifact_failure_terminalizes_current_dispatch(tmp_path: Path) -> None:
    store = FakeDispatchStore()

    with pytest.raises(ValueError, match="digest mismatch"):
        await run_executor_dispatch(
            _supervisor(tmp_path, content=b"wrong content"),
            store,
            executor_dispatch_id="dispatch-1",
            dispatch=_dispatch(digest="0" * 64),
            process_payload=_process_payload(),
        )

    assert len(store.claimed) == 1
    assert store.terminalized == [store.authority]
    assert store.finished == []


@pytest.mark.asyncio
async def test_failed_executor_terminalizes_dispatch(tmp_path: Path) -> None:
    script = b"raise SystemExit(2)"
    digest = hashlib.sha256(script).hexdigest()
    store = FakeDispatchStore()

    with pytest.raises(RuntimeError, match="exited with status 2"):
        await run_executor_dispatch(
            _supervisor(tmp_path, content=script),
            store,
            executor_dispatch_id="dispatch-1",
            dispatch=_dispatch(digest=digest),
            process_payload=_process_payload(),
        )

    assert store.terminalized == [store.authority]
    assert store.finished == []


@pytest.mark.asyncio
@pytest.mark.parametrize("executor_dispatch_id", [None, ""], ids=["missing", "empty"])
async def test_launch_executor_rejects_invalid_dispatch_id_without_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    executor_dispatch_id: str | None,
) -> None:
    script = b"print('must not run')"
    digest = hashlib.sha256(script).hexdigest()
    store = FakeDispatchStore()
    s3_client = FakeS3Client(script)
    supervisor = ExecutorSupervisor(
        cache_dir=tmp_path,
        s3_client=s3_client,
        python_executable=sys.executable,
    )

    async def unexpected_run(*args: object, **kwargs: object) -> None:
        pytest.fail("executor must not run without a dispatch ID")

    monkeypatch.setattr(supervisor, "run", unexpected_run)
    monkeypatch.setattr(supervisor_module, "supervisor", supervisor)
    monkeypatch.setattr(supervisor_module, "dispatch_store", store)
    capture_exception = Mock()
    monkeypatch.setattr(host_observability.sentry_sdk, "capture_exception", capture_exception)

    with pytest.raises(ValueError, match="executor_dispatch_id is required"):
        await supervisor_module.launch_executor.original_func(
            start_benchmark_request_json={},
            benchmark_id_str="benchmark-1",
            verified_task_ids=[],
            executor_dispatch_id=cast(str, executor_dispatch_id),
            executor_release_id="release-v2",
            executor_artifact_uri="s3://artifacts/executors/v2.pex",
            executor_artifact_digest=digest,
            executor_protocol_version="1",
        )

    assert store.claimed == []
    assert store.finished == []
    assert s3_client.calls == []
    capture_exception.assert_called_once()


@pytest.mark.asyncio
async def test_broker_payload_dispatch_id_reaches_dispatch_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def capture_dispatch(*args: object, **kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(supervisor_module, "run_executor_dispatch", capture_dispatch)

    await supervisor_module.launch_executor.original_func(
        start_benchmark_request_json={},
        benchmark_id_str="benchmark-1",
        verified_task_ids=[],
        executor_dispatch_id="dispatch-1",
        executor_release_id="release-v2",
        executor_artifact_uri="s3://artifacts/executors/v2.pex",
        executor_artifact_digest="0" * 64,
        executor_protocol_version="1",
    )

    assert captured["executor_dispatch_id"] == "dispatch-1"


@pytest.mark.asyncio
async def test_launch_executor_records_cancellation_before_context_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def cancel_dispatch(*_args: object, **_kwargs: object) -> None:
        raise asyncio.CancelledError

    cancellation_context: dict[str, object] = {}

    def record_cancellation(telemetry_context: object) -> None:
        cancellation_context.update(
            {
                "telemetry_context": telemetry_context,
                "benchmark_id": host_observability.benchmark_id_var.get(),
                "dispatch_id": host_observability.dispatch_id_var.get(),
            }
        )

    monkeypatch.setattr(supervisor_module, "run_executor_dispatch", cancel_dispatch)
    monkeypatch.setattr(supervisor_module, "record_dispatch_cancellation", record_cancellation)

    with pytest.raises(asyncio.CancelledError):
        await supervisor_module.launch_executor.original_func(
            start_benchmark_request_json={},
            benchmark_id_str="benchmark-1",
            verified_task_ids=[],
            executor_dispatch_id="dispatch-1",
            executor_release_id="release-v2",
            executor_artifact_uri="s3://artifacts/executors/v2.pex",
            executor_artifact_digest="0" * 64,
            executor_protocol_version="1",
        )

    assert cancellation_context["benchmark_id"] == "benchmark-1"
    assert cancellation_context["dispatch_id"] == "dispatch-1"
    assert host_observability.benchmark_id_var.get() == ""


async def test_stream_message_is_deleted_only_after_successful_ack(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        supervisor_module,
        "Redis",
        partial(MockRedis, commands=commands, eval_result=1),
    )
    broker = DeleteAfterAckRedisStreamBroker(
        url="redis://localhost:6379",
        queue_name="default-stream",
        consumer_group_name="executor-group",
    )

    acknowledge = broker._ack_generator(  # pyright: ignore[reportPrivateUsage]
        id="1700000000000-0", queue_name="selected-stream"
    )
    assert commands == []

    await acknowledge()

    assert len(commands) == 1

    command = commands[0]
    assert command[0] == "eval"
    assert command[2:] == (1, "selected-stream", "executor-group", "1700000000000-0")

    script = cast(str, command[1])
    assert " ".join(script.split()) == (
        'local acknowledged = redis.call("XACK", KEYS[1], ARGV[1], ARGV[2]) '
        "if acknowledged == 1 then "
        'redis.call("XDEL", KEYS[1], ARGV[2]) '
        "end return acknowledged"
    )
    await broker.shutdown()


@pytest.mark.parametrize("ack_result", [0, RuntimeError("redis unavailable")], ids=["not-acked", "ack-error"])
async def test_stream_message_is_not_deleted_when_ack_fails(
    monkeypatch: pytest.MonkeyPatch,
    ack_result: int | Exception,
) -> None:
    commands: list[tuple[object, ...]] = []
    monkeypatch.setattr(
        supervisor_module,
        "Redis",
        partial(MockRedis, commands=commands, eval_result=ack_result),
    )
    broker = DeleteAfterAckRedisStreamBroker(
        url="redis://localhost:6379",
        queue_name="default-stream",
        consumer_group_name="executor-group",
    )
    acknowledge = broker._ack_generator(  # pyright: ignore[reportPrivateUsage]
        id="1700000000000-0", queue_name="selected-stream"
    )

    if isinstance(ack_result, Exception):
        with pytest.raises(RuntimeError, match="redis unavailable"):
            await acknowledge()
    else:
        assert await acknowledge() is None

    assert len(commands) == 1
    assert commands[0][0] == "eval"
    await broker.shutdown()


def test_executor_host_uses_one_taskiq_process() -> None:
    dockerfile = (Path(__file__).parents[3] / "services" / "executor_host" / "Dockerfile").read_text()

    assert '"--workers", "1"' in dockerfile


def _confirmed_protection_update(*, enabled: bool = True, expiration: datetime | None = None) -> Any:
    if enabled and expiration is None:
        expiration = datetime.now(tz=UTC) + timedelta(hours=2)
    return supervisor_module._TaskProtectionUpdate(  # pyright: ignore[reportPrivateUsage]
        confirmed=True,
        protection_enabled=enabled,
        expiration=expiration,
    )


def _rejected_protection_update(reason: str) -> Any:
    return supervisor_module._TaskProtectionUpdate(  # pyright: ignore[reportPrivateUsage]
        confirmed=False,
        rejection_reason=reason,
    )


class _ProtectionResponse:
    def __init__(self, body: dict[str, object]) -> None:
        self.body = body

    def __enter__(self) -> _ProtectionResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self.body).encode()


@pytest.mark.asyncio
async def test_task_protection_requires_confirmed_renewable_two_hour_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_bodies: list[dict[str, object]] = []
    expiration = datetime.now(tz=UTC) + timedelta(hours=2)

    def fake_urlopen(request: object, *, timeout: int) -> _ProtectionResponse:
        assert timeout == 5
        request_bodies.append(json.loads(cast(bytes, getattr(request, "data"))))
        return _ProtectionResponse(
            {
                "protection": {
                    "ExpirationDate": expiration.isoformat(),
                    "ProtectionEnabled": True,
                    "TaskArn": "arn:aws:ecs:us-east-1:123456789012:task/cluster/task-1",
                }
            }
        )

    monkeypatch.setattr(supervisor_module, "ECS_AGENT_URI", "http://ecs-agent")
    monkeypatch.setattr(supervisor_module.urllib.request, "urlopen", fake_urlopen)

    update = await supervisor_module._set_task_protection(enabled=True)  # pyright: ignore[reportPrivateUsage]

    assert update.confirmed
    assert update.protection_enabled is True
    assert update.expiration == expiration
    assert request_bodies == [{"ProtectionEnabled": True, "ExpiresInMinutes": 120}]


@pytest.mark.asyncio
async def test_http_200_task_protection_failure_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(_request: object, *, timeout: int) -> _ProtectionResponse:
        assert timeout == 5
        return _ProtectionResponse(
            {
                "failure": {
                    "Arn": "arn:aws:ecs:us-east-1:123456789012:task/cluster/task-1",
                    "Detail": "protected tasks are blocking deployment",
                    "Reason": "DEPLOYMENT_BLOCKED",
                }
            }
        )

    monkeypatch.setattr(supervisor_module, "ECS_AGENT_URI", "http://ecs-agent")
    monkeypatch.setattr(supervisor_module.urllib.request, "urlopen", fake_urlopen)

    update = await supervisor_module._set_task_protection(enabled=True)  # pyright: ignore[reportPrivateUsage]

    assert not update.confirmed
    assert update.rejection_reason == "deployment_blocked"


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (b"not-json", "malformed_response"),
        (
            json.dumps(
                {
                    "protection": {
                        "ExpirationDate": "2099-01-01T00:00:00Z",
                        "ProtectionEnabled": True,
                        "TaskArn": "arn:task",
                    },
                    "failure": {"Reason": "DEPLOYMENT_BLOCKED"},
                }
            ).encode(),
            "ambiguous_response",
        ),
        (
            json.dumps(
                {
                    "protection": {
                        "ExpirationDate": None,
                        "ProtectionEnabled": False,
                        "TaskArn": "arn:task",
                    }
                }
            ).encode(),
            "state_not_confirmed",
        ),
    ],
    ids=["malformed", "ambiguous", "unconfirmed-state"],
)
def test_task_protection_rejects_malformed_ambiguous_or_unconfirmed_responses(
    response: bytes,
    reason: str,
) -> None:
    update = supervisor_module._parse_task_protection_response(  # pyright: ignore[reportPrivateUsage]
        response,
        expected_enabled=True,
    )

    assert not update.confirmed
    assert update.rejection_reason == reason


@pytest.mark.asyncio
async def test_task_protection_retries_failed_refreshes(monkeypatch: pytest.MonkeyPatch) -> None:
    expiration = datetime.now(tz=UTC) + timedelta(hours=2)
    protection_results = iter(
        [
            _rejected_protection_update("deployment_blocked"),
            _confirmed_protection_update(expiration=expiration),
        ]
    )
    delays: list[float] = []

    async def record_protection(*, enabled: bool) -> Any:
        assert enabled
        return next(protection_results)

    async def record_sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 3:
            raise RuntimeError("stop renewal test")

    monkeypatch.setattr(supervisor_module, "_set_task_protection", record_protection)
    monkeypatch.setattr(supervisor_module.asyncio, "sleep", record_sleep)

    with pytest.raises(RuntimeError, match="stop renewal test"):
        await supervisor_module._renew_task_protection(30 * 60)  # pyright: ignore[reportPrivateUsage]

    assert delays == [30 * 60, 30, 30 * 60]
    assert supervisor_module._confirmed_protection_expiration == expiration  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
async def test_task_protection_waits_for_in_flight_refresh_before_cancelling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refresh_started = asyncio.Event()
    finish_refresh = asyncio.Event()
    refresh_completed = asyncio.Event()

    async def block_to_thread(*_args: object, **_kwargs: object) -> bytes:
        refresh_started.set()
        await finish_refresh.wait()
        refresh_completed.set()
        return b"{}"

    monkeypatch.setattr(supervisor_module, "ECS_AGENT_URI", "http://ecs-agent")
    monkeypatch.setattr(supervisor_module.asyncio, "to_thread", block_to_thread)

    refresh_task = asyncio.create_task(
        supervisor_module._set_task_protection(enabled=True)  # pyright: ignore[reportPrivateUsage]
    )
    await refresh_started.wait()
    refresh_task.cancel()
    await asyncio.sleep(0)
    assert not refresh_task.done()

    finish_refresh.set()
    with pytest.raises(asyncio.CancelledError):
        await refresh_task
    assert refresh_completed.is_set()


@pytest.mark.asyncio
async def test_task_protection_has_one_loop_for_concurrent_work(monkeypatch: pytest.MonkeyPatch) -> None:
    protection_calls: list[bool] = []

    async def record_protection(*, enabled: bool) -> Any:
        protection_calls.append(enabled)
        return _confirmed_protection_update(enabled=enabled)

    monkeypatch.setattr(supervisor_module, "_set_task_protection", record_protection)

    await supervisor_module._acquire_task_protection()  # pyright: ignore[reportPrivateUsage]
    refresh_task = supervisor_module._protection_refresh_task  # pyright: ignore[reportPrivateUsage]
    await supervisor_module._acquire_task_protection()  # pyright: ignore[reportPrivateUsage]
    assert supervisor_module._protection_refresh_task is refresh_task  # pyright: ignore[reportPrivateUsage]

    await supervisor_module._release_task_protection()  # pyright: ignore[reportPrivateUsage]
    assert protection_calls == [True]
    await supervisor_module._release_task_protection()  # pyright: ignore[reportPrivateUsage]

    assert protection_calls == [True, False]
    assert refresh_task is not None and refresh_task.done()
    assert supervisor_module._protection_refresh_task is None  # pyright: ignore[reportPrivateUsage]


@pytest.mark.asyncio
async def test_initial_protection_rejection_keeps_one_dispatch_unowned_until_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    protection_calls: list[bool] = []
    retry_started = asyncio.Event()
    allow_recovery = asyncio.Event()
    process_started = asyncio.Event()

    async def set_protection(*, enabled: bool) -> Any:
        protection_calls.append(enabled)
        if not enabled:
            return _confirmed_protection_update(enabled=False)
        if protection_calls == [True]:
            return _rejected_protection_update("deployment_blocked")
        retry_started.set()
        await allow_recovery.wait()
        return _confirmed_protection_update()

    monkeypatch.setattr(supervisor_module, "_set_task_protection", set_protection)
    monkeypatch.setattr(supervisor_module, "_PROTECTION_RETRY_SECONDS", 0)
    script = b"print('ok')"
    digest = hashlib.sha256(script).hexdigest()
    store = FakeDispatchStore()
    executor_supervisor = _supervisor(tmp_path, content=script)

    async def run_executor(*_args: object, **_kwargs: object) -> None:
        process_started.set()

    monkeypatch.setattr(executor_supervisor, "run", run_executor)
    dispatch_task = asyncio.create_task(
        run_executor_dispatch(
            executor_supervisor,
            store,
            executor_dispatch_id="dispatch-1",
            dispatch=_dispatch(digest=digest),
            process_payload=_process_payload(),
        )
    )

    await retry_started.wait()
    assert not dispatch_task.done()
    assert store.claimed == []
    assert cast(FakeS3Client, executor_supervisor.s3_client).calls == []
    assert not process_started.is_set()

    allow_recovery.set()
    await dispatch_task

    assert protection_calls == [True, True, False]
    assert len(store.claimed) == 1
    assert len(store.finished) == 1
    assert len(cast(FakeS3Client, executor_supervisor.s3_client).calls) == 1
    assert process_started.is_set()


@pytest.mark.asyncio
async def test_renewal_rejection_blocks_new_admission_without_stopping_existing_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_expiration = datetime.now(tz=UTC) + timedelta(hours=2)
    recovered_expiration = first_expiration + timedelta(minutes=30)
    renewal_rejected = asyncio.Event()
    allow_recovery = asyncio.Event()
    existing_started = asyncio.Event()
    finish_existing = asyncio.Event()
    second_claimed = asyncio.Event()
    protection_attempt = 0
    rejection_telemetry: list[tuple[str, datetime | None]] = []

    async def set_protection(*, enabled: bool) -> Any:
        nonlocal protection_attempt
        if not enabled:
            return _confirmed_protection_update(enabled=False)
        protection_attempt += 1
        if protection_attempt == 1:
            return _confirmed_protection_update(expiration=first_expiration)
        if protection_attempt == 2:
            renewal_rejected.set()
            return _rejected_protection_update("deployment_blocked")
        await allow_recovery.wait()
        return _confirmed_protection_update(expiration=recovered_expiration)

    def record_rejection(*, reason: str, confirmed_expiration: datetime | None) -> None:
        rejection_telemetry.append((reason, confirmed_expiration))

    async def prepare_artifact(_dispatch: ArtifactDispatch) -> Path:
        return tmp_path / "executor.pex"

    first_supervisor = _supervisor(tmp_path / "first", content=b"unused")
    second_supervisor = _supervisor(tmp_path / "second", content=b"unused")

    async def run_existing(*_args: object, **_kwargs: object) -> None:
        existing_started.set()
        await finish_existing.wait()

    async def run_second(*_args: object, **_kwargs: object) -> None:
        return None

    first_store = FakeDispatchStore()
    second_store = FakeDispatchStore()
    original_second_claim = second_store.claim

    async def record_second_claim(
        dispatch_id: str,
        benchmark_id: str,
        dispatch: ArtifactDispatch,
    ) -> DispatchAuthority | None:
        second_claimed.set()
        return await original_second_claim(dispatch_id, benchmark_id, dispatch)

    monkeypatch.setattr(supervisor_module, "_set_task_protection", set_protection)
    monkeypatch.setattr(supervisor_module, "record_task_protection_rejection", record_rejection)
    monkeypatch.setattr(supervisor_module, "_PROTECTION_REFRESH_SECONDS", 0.01)
    monkeypatch.setattr(supervisor_module, "_PROTECTION_RETRY_SECONDS", 0.01)
    monkeypatch.setattr(first_supervisor, "prepare_artifact", prepare_artifact)
    monkeypatch.setattr(first_supervisor, "run", run_existing)
    monkeypatch.setattr(second_supervisor, "prepare_artifact", prepare_artifact)
    monkeypatch.setattr(second_supervisor, "run", run_second)
    monkeypatch.setattr(second_store, "claim", record_second_claim)
    dispatch = _dispatch(digest="0" * 64)

    existing_task = asyncio.create_task(
        run_executor_dispatch(
            first_supervisor,
            first_store,
            executor_dispatch_id="dispatch-1",
            dispatch=dispatch,
            process_payload=_process_payload(),
        )
    )
    await existing_started.wait()
    await renewal_rejected.wait()

    new_task = asyncio.create_task(
        run_executor_dispatch(
            second_supervisor,
            second_store,
            executor_dispatch_id="dispatch-2",
            dispatch=dispatch,
            process_payload=_process_payload(),
        )
    )
    await asyncio.sleep(0)
    assert not existing_task.done()
    assert second_store.claimed == []
    assert rejection_telemetry == [("deployment_blocked", first_expiration)]
    assert supervisor_module._confirmed_protection_expiration == first_expiration  # pyright: ignore[reportPrivateUsage]

    allow_recovery.set()
    await asyncio.wait_for(second_claimed.wait(), timeout=1)
    await new_task
    assert supervisor_module._confirmed_protection_expiration == recovered_expiration  # pyright: ignore[reportPrivateUsage]
    assert not existing_task.done()

    finish_existing.set()
    await existing_task


@pytest.mark.asyncio
async def test_task_protection_is_acquired_before_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    store = FakeDispatchStore(claim_result=False)
    original_claim = store.claim

    async def record_protection(*, enabled: bool) -> Any:
        events.append(f"protection-{enabled}")
        return _confirmed_protection_update(enabled=enabled)

    async def record_claim(
        dispatch_id: str,
        benchmark_id: str,
        dispatch: ArtifactDispatch,
    ) -> DispatchAuthority | None:
        events.append("claim")
        return await original_claim(dispatch_id, benchmark_id, dispatch)

    monkeypatch.setattr(supervisor_module, "_set_task_protection", record_protection)
    monkeypatch.setattr(store, "claim", record_claim)

    artifact = b"unused"
    await run_executor_dispatch(
        _supervisor(tmp_path, content=artifact),
        store,
        executor_dispatch_id="dispatch-1",
        dispatch=_dispatch(digest=hashlib.sha256(artifact).hexdigest()),
        process_payload=_process_payload(),
    )

    assert events == ["protection-True", "claim", "protection-False"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("repeat_acquisition_cancellation", "repeat_release_cancellation"),
    [(False, False), (True, False), (False, True)],
    ids=["single", "repeated-acquisition", "repeated-release"],
)
async def test_cancellation_during_protection_acquisition_releases_before_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    repeat_acquisition_cancellation: bool,
    repeat_release_cancellation: bool,
) -> None:
    protection_calls: list[bool] = []
    enable_started = asyncio.Event()
    finish_enable = asyncio.Event()
    release_started = asyncio.Event()
    finish_release = asyncio.Event()
    release_completed = asyncio.Event()
    store = FakeDispatchStore(claim_result=False)

    async def block_task_protection(*, enabled: bool) -> Any:
        protection_calls.append(enabled)
        if enabled and protection_calls == [True]:
            enable_started.set()
            await finish_enable.wait()
        elif not enabled and repeat_release_cancellation and protection_calls == [True, False]:
            release_started.set()
            await finish_release.wait()
            release_completed.set()
        return _confirmed_protection_update(enabled=enabled)

    monkeypatch.setattr(supervisor_module, "_set_task_protection", block_task_protection)
    artifact = b"unused"
    dispatch = _dispatch(digest=hashlib.sha256(artifact).hexdigest())
    task = asyncio.create_task(
        run_executor_dispatch(
            _supervisor(tmp_path, content=artifact),
            store,
            executor_dispatch_id="dispatch-1",
            dispatch=dispatch,
            process_payload=_process_payload(),
        )
    )
    await enable_started.wait()
    task.cancel()
    if repeat_acquisition_cancellation:
        asyncio.get_running_loop().call_soon(task.cancel)
    asyncio.get_running_loop().call_soon(finish_enable.set)
    if repeat_release_cancellation:
        await release_started.wait()
        task.cancel()
        finish_release.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert protection_calls == [True, False]
    assert release_completed.is_set() is repeat_release_cancellation
    assert supervisor_module._active_execution_count == 0  # pyright: ignore[reportPrivateUsage]
    assert store.claimed == []
    assert store.finished == []
    assert store.terminalized == []

    await run_executor_dispatch(
        _supervisor(tmp_path, content=artifact),
        store,
        executor_dispatch_id="dispatch-2",
        dispatch=dispatch,
        process_payload=_process_payload(),
    )

    assert protection_calls == [True, False, True, False]
    assert supervisor_module._active_execution_count == 0  # pyright: ignore[reportPrivateUsage]
    assert len(store.claimed) == 1


@pytest.mark.asyncio
async def test_cancellation_after_claim_terminalizes_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = b"unused"
    store = FakeDispatchStore()
    supervisor = _supervisor(tmp_path, content=artifact)
    entered_run = asyncio.Event()

    async def block_run(*args: object, **kwargs: object) -> None:
        entered_run.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(supervisor, "run", block_run)
    task = asyncio.create_task(
        run_executor_dispatch(
            supervisor,
            store,
            executor_dispatch_id="dispatch-1",
            dispatch=_dispatch(digest=hashlib.sha256(artifact).hexdigest()),
            process_payload=_process_payload(),
        )
    )
    await entered_run.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert store.terminalized == [store.authority]
    assert store.finished == []


@pytest.mark.asyncio
async def test_periodic_authority_operational_error_allows_child_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeProcess:
        returncode: int | None = None

        def __init__(self) -> None:
            self.done = asyncio.Event()

        async def wait(self) -> int:
            await self.done.wait()
            assert self.returncode is not None
            return self.returncode

    process = FakeProcess()
    sleep_count = 0
    authority_blocker = asyncio.Event()

    async def sleep(_delay: float) -> None:
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count == 3:
            process.returncode = 0
            process.done.set()
            await authority_blocker.wait()

    checks = iter(
        [
            supervisor_module.psycopg2.OperationalError("temporary"),
            True,
            True,
        ]
    )

    async def is_current() -> bool:
        result = next(checks)
        if isinstance(result, BaseException):
            raise result
        return result

    terminate = Mock()
    monkeypatch.setattr(supervisor_module, "_terminate_process_group", terminate)
    supervisor = _supervisor(tmp_path, content=b"unused", sleep=sleep)

    assert (
        await supervisor._wait_with_authority(  # pyright: ignore[reportPrivateUsage]
            cast(asyncio.subprocess.Process, process),
            is_current,
            asyncio.Event(),
        )
        == 0
    )
    terminate.assert_not_called()


@pytest.mark.asyncio
async def test_heartbeat_lease_loss_terminates_process_and_cleans_up_tasks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeProcess:
        pid = 123
        returncode: int | None = None

        def __init__(self) -> None:
            self.done = asyncio.Event()

        async def wait(self) -> int:
            await self.done.wait()
            assert self.returncode is not None
            return self.returncode

    process = FakeProcess()
    lease_lost = asyncio.Event()
    lease_lost.set()
    created_tasks: list[asyncio.Task[object]] = []
    create_task = asyncio.create_task

    def record_task(coroutine: Coroutine[Any, Any, object]) -> asyncio.Task[object]:
        task = create_task(coroutine)
        created_tasks.append(task)
        return task

    async def is_current() -> bool:
        return True

    async def terminate_process(_process: object) -> None:
        process.returncode = -15
        process.done.set()

    monkeypatch.setattr(supervisor_module.asyncio, "create_task", record_task)
    monkeypatch.setattr(supervisor_module, "_terminate_process_group", terminate_process)
    supervisor = _supervisor(tmp_path, content=b"unused", sleep=lambda _delay: asyncio.sleep(0))

    with pytest.raises(DispatchAuthorityLostError, match="lease expired"):
        await supervisor._wait_with_authority(  # pyright: ignore[reportPrivateUsage]
            cast(asyncio.subprocess.Process, process),
            is_current,
            lease_lost,
        )

    assert process.returncode == -15
    assert len(created_tasks) == 3
    assert all(task.done() for task in created_tasks)


@pytest.mark.asyncio
async def test_periodic_authority_operational_error_then_loss_terminates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeProcess:
        pid = 123
        returncode: int | None = None

        def __init__(self) -> None:
            self.done = asyncio.Event()

        async def wait(self) -> int:
            await self.done.wait()
            assert self.returncode is not None
            return self.returncode

    process = FakeProcess()
    checks = iter([supervisor_module.psycopg2.OperationalError("temporary"), False])

    async def is_current() -> bool:
        result = next(checks)
        if isinstance(result, BaseException):
            raise result
        return result

    async def terminate_process(_process: object) -> None:
        process.returncode = -15
        process.done.set()

    monkeypatch.setattr(supervisor_module, "_AUTHORITY_LOSS_GRACE_SECONDS", 0)
    monkeypatch.setattr(supervisor_module, "_terminate_process_group", terminate_process)
    supervisor = _supervisor(tmp_path, content=b"unused", sleep=lambda _delay: asyncio.sleep(0))

    with pytest.raises(DispatchAuthorityLostError, match="superseded"):
        await supervisor._wait_with_authority(  # pyright: ignore[reportPrivateUsage]
            cast(asyncio.subprocess.Process, process),
            is_current,
            asyncio.Event(),
        )

    assert process.returncode == -15


@pytest.mark.asyncio
async def test_unexpected_periodic_authority_error_propagates(tmp_path: Path) -> None:
    async def is_current() -> bool:
        raise RuntimeError("unexpected")

    supervisor = _supervisor(
        tmp_path,
        content=b"unused",
        sleep=lambda _delay: asyncio.sleep(0),
    )

    with pytest.raises(RuntimeError, match="unexpected"):
        await supervisor._wait_for_authority_loss(  # pyright: ignore[reportPrivateUsage]
            is_current
        )


@pytest.mark.asyncio
async def test_authority_revocation_terminates_process_before_terminalization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeProcess:
        pid = 123
        returncode: int | None = None

        def __init__(self) -> None:
            self.done = asyncio.Event()

        async def wait(self) -> int:
            await self.done.wait()
            assert self.returncode is not None
            return self.returncode

    process = FakeProcess()
    authority_check_due = asyncio.Event()
    trigger_authority_check = asyncio.Event()
    lifecycle_events: list[str] = []
    store = FakeDispatchStore(authority_results=[True, False])
    original_terminalize = store.terminalize

    async def explicit_authority_check(_delay: float) -> None:
        authority_check_due.set()
        await trigger_authority_check.wait()

    async def create_process(*args: object, **kwargs: object) -> object:
        return process

    async def terminate_process(_process: object) -> None:
        if process.returncode is None:
            lifecycle_events.append("terminate")
            process.returncode = -15
            process.done.set()

    async def record_terminalize(authority: DispatchAuthority, task_ids: list[str]) -> bool:
        lifecycle_events.append("terminalize")
        return await original_terminalize(authority, task_ids)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(supervisor_module, "_AUTHORITY_LOSS_GRACE_SECONDS", 0)
    monkeypatch.setattr(supervisor_module, "_terminate_process_group", terminate_process)
    monkeypatch.setattr(store, "terminalize", record_terminalize)
    script = b"print('not executed by fake process')"
    digest = hashlib.sha256(script).hexdigest()
    task = asyncio.create_task(
        run_executor_dispatch(
            _supervisor(tmp_path, content=script, sleep=explicit_authority_check),
            store,
            executor_dispatch_id="dispatch-1",
            dispatch=_dispatch(digest=digest),
            process_payload=_process_payload(),
        )
    )
    await authority_check_due.wait()
    trigger_authority_check.set()

    with pytest.raises(DispatchAuthorityLostError, match="superseded"):
        await task

    assert lifecycle_events == ["terminate", "terminalize"]
    assert store.authority_checks == [store.authority, store.authority]


@pytest.mark.asyncio
async def test_stale_successful_finish_cannot_finish_dispatch(tmp_path: Path) -> None:
    script = b"print('ok')"
    digest = hashlib.sha256(script).hexdigest()
    store = FakeDispatchStore(finish_result=False)

    await run_executor_dispatch(
        _supervisor(tmp_path, content=script),
        store,
        executor_dispatch_id="dispatch-1",
        dispatch=_dispatch(digest=digest),
        process_payload=_process_payload(),
    )

    assert store.finished == [store.authority]
    assert store.terminalized == []


@pytest.mark.asyncio
async def test_prepare_artifact_rejects_download_digest_mismatch(tmp_path: Path) -> None:
    client = FakeS3Client(b"wrong content")
    supervisor = ExecutorSupervisor(
        cache_dir=tmp_path,
        s3_client=client,
        artifact_bucket="artifacts",
        artifact_prefix="executors",
    )

    with pytest.raises(ValueError, match="digest mismatch"):
        await supervisor.prepare_artifact(_dispatch(digest="0" * 64))

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("protocol_version", ["1", "2", "3"])
def test_host_accepts_current_and_pinned_legacy_protocols(protocol_version: str) -> None:
    dispatch = ArtifactDispatch.from_payload(
        {
            "executor_release_id": "immutable-release",
            "executor_artifact_uri": "s3://artifacts/releases/immutable.pex",
            "executor_artifact_digest": "a" * 64,
            "executor_protocol_version": protocol_version,
        }
    )
    assert dispatch.protocol_version == protocol_version
    assert dispatch.release_id == "immutable-release"
