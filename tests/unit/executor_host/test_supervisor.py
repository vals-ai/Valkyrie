from __future__ import annotations

import asyncio
import hashlib
import json
from json import JSONDecodeError
import logging
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import cast

import pytest

import services.executor_host.supervisor as supervisor_module
from services.executor_host.supervisor import (  # pyright: ignore[reportMissingImports]
    ArtifactDispatch,
    DispatchAuthority,
    DispatchAuthorityLostError,
    ExecutorSupervisor,
    PostgresExecutorDispatchStore,
    run_executor_dispatch,
    verify_file_digest,
)
from executor_protocol import validate_executor_artifact_uri


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


def _dispatch(*, digest: str) -> ArtifactDispatch:
    return ArtifactDispatch.from_payload(
        {
            "executor_release_id": "release-v2",
            "executor_artifact_uri": "s3://artifacts/executors/v2.pex",
            "executor_artifact_digest": digest,
            "executor_protocol_version": "1",
        }
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
    assert "SET status = 'RUNNING'" in statement
    assert "started_at = CURRENT_TIMESTAMP" in statement
    assert parameters[:2] == ("dispatch-1", "benchmark-1")


@pytest.mark.asyncio
async def test_postgres_authority_and_completion_are_fenced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = RecordingCursor([(True,), ("FINISHED",), ("dispatch-1",)])
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
    assert await store.finish(authority)

    authority_statement, authority_parameters = cursor.statements[0]
    finish_lock_statement, finish_lock_parameters = cursor.statements[1]
    finish_statement, finish_parameters = cursor.statements[2]
    assert "dispatch.status = 'RUNNING'" in authority_statement
    assert "benchmark.status != 'STOPPED'" in authority_statement
    assert authority_parameters == ("dispatch-1",)
    assert "FOR UPDATE" in finish_lock_statement
    assert finish_lock_parameters == ("benchmark-1",)
    assert "SET status = 'FINISHED'" in finish_statement
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
    script = b"""import json, os, sys\nfrom pathlib import Path\npayload = json.loads(Path(sys.argv[1]).read_text())\nPath(os.environ[\"EXECUTOR_TEST_MARKER\"]).write_text(json.dumps(payload))\n"""
    digest = hashlib.sha256(script).hexdigest()
    marker = tmp_path / "marker.json"
    monkeypatch.setenv("EXECUTOR_TEST_MARKER", str(marker))
    store = FakeDispatchStore()

    with caplog.at_level(logging.INFO, logger=supervisor_module.logger.name):
        await run_executor_dispatch(
            _supervisor(tmp_path, content=script),
            store,
            executor_dispatch_id="dispatch-1",
            dispatch=_dispatch(digest=digest),
            start_benchmark_request_json={"benchmark_name": "swebench"},
            benchmark_id_str="benchmark-1",
            verified_task_ids=["task-1"],
        )

    try:
        result = json.loads(marker.read_text())
    except (OSError, JSONDecodeError) as error:
        raise AssertionError(f"executor marker was not valid JSON: {error}") from error
    payload = result
    assert payload["benchmark_id_str"] == "benchmark-1"
    assert payload["verified_task_ids"] == ["task-1"]
    assert payload["executor_dispatch_id"] == "dispatch-1"
    assert store.authority_checks
    assert store.finished == [store.authority]
    assert (
        f"Launching benchmark benchmark-1 dispatch_id=dispatch-1 release=release-v2 digest={digest} protocol=1"
    ) in caplog.messages


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
        start_benchmark_request_json={},
        benchmark_id_str="benchmark-1",
        verified_task_ids=[],
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
        start_benchmark_request_json={},
        benchmark_id_str="benchmark-1",
        verified_task_ids=[],
    )
    await run_executor_dispatch(
        supervisor,
        store,
        executor_dispatch_id="dispatch-1",
        dispatch=_dispatch(digest=digest),
        start_benchmark_request_json={},
        benchmark_id_str="benchmark-1",
        verified_task_ids=[],
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
            start_benchmark_request_json={},
            benchmark_id_str="benchmark-1",
            verified_task_ids=[],
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
            start_benchmark_request_json={},
            benchmark_id_str="benchmark-1",
            verified_task_ids=[],
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


def test_executor_host_uses_one_taskiq_process() -> None:
    dockerfile = (Path(__file__).parents[3] / "services" / "executor_host" / "Dockerfile").read_text()

    assert '"--workers", "1"' in dockerfile


@pytest.mark.asyncio
async def test_task_protection_uses_a_renewable_two_hour_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    request_bodies: list[dict[str, object]] = []

    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return b""

    def fake_urlopen(request: object, *, timeout: int) -> Response:
        assert timeout == 5
        request_bodies.append(json.loads(cast(bytes, getattr(request, "data"))))
        return Response()

    monkeypatch.setattr(supervisor_module, "ECS_AGENT_URI", "http://ecs-agent")
    monkeypatch.setattr(supervisor_module.urllib.request, "urlopen", fake_urlopen)

    set_task_protection = getattr(supervisor_module, "_set_task_protection")
    assert await set_task_protection(enabled=True)
    assert request_bodies == [{"ProtectionEnabled": True, "ExpiresInMinutes": 120}]


@pytest.mark.asyncio
async def test_task_protection_retries_failed_refreshes(monkeypatch: pytest.MonkeyPatch) -> None:
    protection_results = iter([False, True])
    delays: list[float] = []

    async def record_protection(*, enabled: bool) -> bool:
        assert enabled
        return next(protection_results)

    async def record_sleep(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 3:
            raise RuntimeError("stop renewal test")

    monkeypatch.setattr(supervisor_module, "_set_task_protection", record_protection)
    monkeypatch.setattr(supervisor_module.asyncio, "sleep", record_sleep)

    renew_task_protection = getattr(supervisor_module, "_renew_task_protection")
    with pytest.raises(RuntimeError, match="stop renewal test"):
        await renew_task_protection(30 * 60)

    assert delays == [30 * 60, 30, 30 * 60]


@pytest.mark.asyncio
async def test_task_protection_waits_for_in_flight_refresh_before_cancelling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    refresh_started = asyncio.Event()
    finish_refresh = asyncio.Event()
    refresh_completed = asyncio.Event()

    async def block_to_thread(*_args: object, **_kwargs: object) -> None:
        refresh_started.set()
        await finish_refresh.wait()
        refresh_completed.set()

    monkeypatch.setattr(supervisor_module, "ECS_AGENT_URI", "http://ecs-agent")
    monkeypatch.setattr(supervisor_module.asyncio, "to_thread", block_to_thread)

    set_task_protection = getattr(supervisor_module, "_set_task_protection")
    refresh_task = asyncio.create_task(set_task_protection(enabled=True))
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

    async def record_protection(*, enabled: bool) -> bool:
        protection_calls.append(enabled)
        return True

    monkeypatch.setattr(supervisor_module, "_set_task_protection", record_protection)

    acquire_task_protection = getattr(supervisor_module, "_acquire_task_protection")
    release_task_protection = getattr(supervisor_module, "_release_task_protection")
    await acquire_task_protection()
    refresh_task = getattr(supervisor_module, "_protection_refresh_task")
    await acquire_task_protection()
    assert getattr(supervisor_module, "_protection_refresh_task") is refresh_task

    await release_task_protection()
    assert protection_calls == [True]
    await release_task_protection()

    assert protection_calls == [True, False]
    assert refresh_task.done()
    assert getattr(supervisor_module, "_protection_refresh_task") is None


@pytest.mark.asyncio
async def test_task_protection_is_acquired_before_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    store = FakeDispatchStore(claim_result=False)
    original_claim = store.claim

    async def record_protection(*, enabled: bool) -> None:
        events.append(f"protection-{enabled}")

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
        start_benchmark_request_json={},
        benchmark_id_str="benchmark-1",
        verified_task_ids=[],
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

    async def block_task_protection(*, enabled: bool) -> None:
        protection_calls.append(enabled)
        if enabled and protection_calls == [True]:
            enable_started.set()
            await finish_enable.wait()
        elif not enabled and repeat_release_cancellation and protection_calls == [True, False]:
            release_started.set()
            await finish_release.wait()
            release_completed.set()

    monkeypatch.setattr(supervisor_module, "_set_task_protection", block_task_protection)
    artifact = b"unused"
    dispatch = _dispatch(digest=hashlib.sha256(artifact).hexdigest())
    task = asyncio.create_task(
        run_executor_dispatch(
            _supervisor(tmp_path, content=artifact),
            store,
            executor_dispatch_id="dispatch-1",
            dispatch=dispatch,
            start_benchmark_request_json={},
            benchmark_id_str="benchmark-1",
            verified_task_ids=[],
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
    assert getattr(supervisor_module, "_active_execution_count") == 0
    assert store.claimed == []
    assert store.finished == []
    assert store.terminalized == []

    await run_executor_dispatch(
        _supervisor(tmp_path, content=artifact),
        store,
        executor_dispatch_id="dispatch-2",
        dispatch=dispatch,
        start_benchmark_request_json={},
        benchmark_id_str="benchmark-1",
        verified_task_ids=[],
    )

    assert protection_calls == [True, False, True, False]
    assert getattr(supervisor_module, "_active_execution_count") == 0
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
            start_benchmark_request_json={},
            benchmark_id_str="benchmark-1",
            verified_task_ids=[],
        )
    )
    await entered_run.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert store.terminalized == [store.authority]
    assert store.finished == []


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
            start_benchmark_request_json={},
            benchmark_id_str="benchmark-1",
            verified_task_ids=[],
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
        start_benchmark_request_json={},
        benchmark_id_str="benchmark-1",
        verified_task_ids=[],
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
