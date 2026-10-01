"""Runner lifecycle and child-process authority behavior."""

from __future__ import annotations

import asyncio
import hashlib
import os
import signal
import sys
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from typing import cast
from uuid import uuid4

import pytest
import sentry_sdk
from sentry_sdk.envelope import Envelope
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.transport import Transport

from executor_protocol import ExecutorTelemetryContext
from tracker.executor import runner


class FakeStore:
    def __init__(self, claim: runner.ClaimedDispatch | None = None) -> None:
        self.available = claim
        self.terminalized: list[tuple[runner.DispatchAuthority, list[str]]] = []
        self.finished: list[runner.DispatchAuthority] = []
        self.renewals: list[runner.DispatchAuthority] = []
        self.preclaim_failures: list[tuple[str, str]] = []

    async def claim(self, dispatch_id: str) -> runner.ClaimedDispatch | None:
        claim, self.available = self.available, None
        return claim

    async def fail_before_claim(self, dispatch_id: str, error_type: str) -> bool:
        self.preclaim_failures.append((dispatch_id, error_type))
        return True

    async def renew(self, authority: runner.DispatchAuthority) -> runner.RenewalResult:
        return runner.RenewalResult(True, (True, False))

    async def finish(self, authority: runner.DispatchAuthority) -> bool:
        self.finished.append(authority)
        return True

    async def terminalize(self, authority: runner.DispatchAuthority, task_ids: list[str]) -> bool:
        self.terminalized.append((authority, task_ids))
        return True


class FakeS3:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.downloads = 0

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        self.downloads += 1
        Path(filename).write_bytes(self.content)


def _claim(content: bytes) -> runner.ClaimedDispatch:
    authority = runner.DispatchAuthority(str(uuid4()), str(uuid4()))
    artifact = runner.ArtifactDispatch.from_payload(
        {
            "executor_release_id": "release-1",
            "executor_artifact_uri": "s3://artifacts/executors/runner.pex",
            "executor_artifact_digest": hashlib.sha256(content).hexdigest(),
            "executor_protocol_version": "3",
        }
    )
    telemetry: ExecutorTelemetryContext = {"request_id": "admitted-request", "trace_headers": {}}
    process = runner.ExecutorProcessPayload.from_payload(
        {
            "start_benchmark_request_json": {"request_id": "admitted-request"},
            "benchmark_id_str": authority.benchmark_id,
            "verified_task_ids": ["task-0"],
        },
        telemetry_context=telemetry,
    )
    return runner.ClaimedDispatch(authority, artifact, process, telemetry)


def _supervisor(cache: Path, content: bytes) -> tuple[runner.ExecutorSupervisor, FakeS3]:
    client = FakeS3(content)
    return runner.ExecutorSupervisor(
        cache,
        s3_client=client,
        python_executable=sys.executable,
        artifact_bucket="artifacts",
        artifact_prefix="executors",
    ), client


@pytest.mark.asyncio
async def test_duplicate_claim_does_not_start_child(tmp_path: Path) -> None:
    script = b"print('ok')"
    claim = _claim(script)
    store = FakeStore(claim)
    supervisor, client = _supervisor(tmp_path, script)
    await runner.run_executor_dispatch(
        supervisor, store, keeper=runner._LeaseKeeper(store), executor_dispatch_id=claim.authority.dispatch_id
    )
    await runner.run_executor_dispatch(
        supervisor, store, keeper=runner._LeaseKeeper(store), executor_dispatch_id=claim.authority.dispatch_id
    )
    assert store.finished == [claim.authority]
    assert client.downloads == 1


@pytest.mark.asyncio
async def test_artifact_digest_failure_terminalizes_claim(tmp_path: Path) -> None:
    claim = _claim(b"correct artifact")
    store = FakeStore(claim)
    supervisor, _ = _supervisor(tmp_path, b"wrong artifact")
    with pytest.raises(ValueError, match="digest mismatch"):
        await runner.run_executor_dispatch(
            supervisor, store, keeper=runner._LeaseKeeper(store), executor_dispatch_id=claim.authority.dispatch_id
        )
    assert store.terminalized == [(claim.authority, ["task-0"])]
    assert store.finished == []


@pytest.mark.asyncio
async def test_cancel_after_claim_terminalizes_before_artifact_preparation(tmp_path: Path) -> None:
    claim = _claim(b"artifact")
    store = FakeStore(claim)
    supervisor, _ = _supervisor(tmp_path, b"artifact")
    started = asyncio.Event()

    async def prepare(_dispatch: runner.ArtifactDispatch) -> Path:
        started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    supervisor.prepare_artifact = prepare  # type: ignore[method-assign]
    task = asyncio.create_task(
        runner.run_executor_dispatch(
            supervisor,
            store,
            keeper=runner._LeaseKeeper(store),
            executor_dispatch_id=claim.authority.dispatch_id,
        )
    )
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert store.terminalized == [(claim.authority, ["task-0"])]


@pytest.mark.asyncio
async def test_sigterm_after_child_spawn_terminalizes_and_kills_process_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = b"import time\nwhile True: time.sleep(1)\n"
    claim = _claim(script)
    store = FakeStore(claim)
    supervisor, _ = _supervisor(tmp_path, script)
    original_spawn = asyncio.create_subprocess_exec
    child_started = asyncio.Event()
    child: asyncio.subprocess.Process | None = None

    async def spawn(*args: object, **kwargs: object) -> asyncio.subprocess.Process:
        nonlocal child
        child = await original_spawn(*args, **kwargs)  # type: ignore[arg-type]
        child_started.set()
        return child

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(runner.PostgresExecutorDispatchStore, "from_environment", lambda: store)
    monkeypatch.setattr(runner, "ExecutorSupervisor", lambda _cache, **_kwargs: supervisor)
    monkeypatch.setenv("EXECUTOR_CACHE_DIR", str(tmp_path))
    loop = asyncio.get_running_loop()
    handlers: dict[signal.Signals, object] = {}
    monkeypatch.setattr(loop, "add_signal_handler", lambda sig, callback: handlers.update({sig: callback}))
    monkeypatch.setattr(loop, "remove_signal_handler", lambda sig: handlers.pop(sig))
    task = asyncio.create_task(runner._run_main(claim.authority.dispatch_id))
    await child_started.wait()
    assert child is not None
    callback = handlers[signal.SIGTERM]
    assert callable(callback)
    callback()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert child.returncode is not None
    with pytest.raises(ProcessLookupError):
        os.killpg(child.pid, 0)
    assert store.terminalized == [(claim.authority, ["task-0"])]


@pytest.mark.parametrize("protocol_version", ["1", "2", "3", "4"])
def test_pinned_protocol_versions_remain_supported(protocol_version: str) -> None:
    assert (
        runner.ArtifactDispatch.from_payload(
            {
                "executor_release_id": "release-1",
                "executor_artifact_uri": "s3://artifacts/executors/runner.pex",
                "executor_artifact_digest": "a" * 64,
                "executor_protocol_version": protocol_version,
            }
        ).protocol_version
        == protocol_version
    )


@pytest.mark.asyncio
async def test_sigterm_handler_cancels_and_awaits_claimed_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claim = _claim(b"artifact")
    store = FakeStore(claim)
    supervisor, _ = _supervisor(tmp_path, b"artifact")
    preparing = asyncio.Event()

    async def prepare(_dispatch: runner.ArtifactDispatch) -> Path:
        preparing.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    supervisor.prepare_artifact = prepare  # type: ignore[method-assign]
    monkeypatch.setattr(runner.PostgresExecutorDispatchStore, "from_environment", lambda: store)
    monkeypatch.setattr(runner, "ExecutorSupervisor", lambda _cache, **_kwargs: supervisor)
    monkeypatch.setenv("EXECUTOR_CACHE_DIR", str(tmp_path))
    loop = asyncio.get_running_loop()
    handlers: dict[signal.Signals, object] = {}
    monkeypatch.setattr(loop, "add_signal_handler", lambda sig, callback: handlers.update({sig: callback}))
    monkeypatch.setattr(loop, "remove_signal_handler", lambda sig: handlers.pop(sig))
    task = asyncio.create_task(runner._run_main(claim.authority.dispatch_id))
    await preparing.wait()
    callback = handlers[signal.SIGTERM]
    assert callable(callback)
    callback()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert store.terminalized == [(claim.authority, ["task-0"])]
    assert signal.SIGTERM not in handlers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "expected_lost", "expected_revoked", "expected_renewed"),
    [
        (runner.RenewalResult(True, (True, False)), False, False, True),
        (runner.RenewalResult(True, (True, True)), False, True, True),
        (runner.RenewalResult(False, (True, False)), False, False, False),
        (runner.RenewalResult(False, (False, True)), False, True, False),
        (runner.RenewalResult(False, (False, False)), True, False, False),
        (runner.RenewalResult(True, None), False, False, True),
    ],
)
async def test_one_renew_tick_classifies_owned_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    outcome: runner.RenewalResult,
    expected_lost: bool,
    expected_revoked: bool,
    expected_renewed: bool,
) -> None:
    store = FakeStore()
    keeper = runner._LeaseKeeper(store)
    now = asyncio.get_running_loop().time()
    authority = runner.DispatchAuthority("dispatch-1", "benchmark-1")
    lease = keeper.register(authority, now)
    worker = keeper.task
    assert worker is not None

    async def renew(requested: runner.DispatchAuthority) -> runner.RenewalResult:
        store.renewals.append(requested)
        return outcome

    monkeypatch.setattr(store, "renew", renew)
    try:
        await keeper.tick()
        assert store.renewals == [authority]
        assert lease.lost.is_set() is expected_lost
        assert lease.revoked.is_set() is expected_revoked
        assert (lease.last_confirmed_renewal_at > now) is expected_renewed
    finally:
        await keeper.unregister()
    assert worker.done()
    assert keeper.task is None


@pytest.mark.asyncio
async def test_lease_keeper_rejects_a_second_dispatch_and_stops_on_exit() -> None:
    store = FakeStore()
    keeper = runner._LeaseKeeper(store, interval_seconds=0.01)
    authority = runner.DispatchAuthority("dispatch-1", "benchmark-1")
    keeper.register(authority, asyncio.get_running_loop().time())
    with pytest.raises(RuntimeError, match="already owns a dispatch"):
        keeper.register(runner.DispatchAuthority("dispatch-2", "benchmark-2"), asyncio.get_running_loop().time())
    worker = keeper.task
    assert worker is not None
    await keeper.unregister()
    assert worker.done()
    assert keeper.task is None


@pytest.mark.asyncio
async def test_older_tick_cannot_rewind_newer_refresh_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FakeStore()
    keeper = runner._LeaseKeeper(store)
    authority = runner.DispatchAuthority("dispatch-1", "benchmark-1")
    now = [asyncio.get_running_loop().time()]
    lease = keeper.register(authority, now[0])
    worker = keeper.task
    assert worker is not None
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def renew(_authority: runner.DispatchAuthority) -> runner.RenewalResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            entered.set()
            await release.wait()
            outcome = runner.RenewalResult(True, (True, True))
        else:
            outcome = runner.RenewalResult(True, (True, False))
        return outcome

    monkeypatch.setattr(store, "renew", renew)
    monkeypatch.setattr(runner, "_monotonic_time", lambda: now[0])
    now[0] += 1
    old_tick = asyncio.create_task(keeper.tick())
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        now[0] += 1
        await keeper.refresh()
        refreshed_at = lease.last_confirmed_renewal_at
        refreshed_timer = keeper.timer
        assert refreshed_at == now[0]
        release.set()
        await old_tick
        assert lease.last_confirmed_renewal_at == refreshed_at
        assert keeper.timer is refreshed_timer
        assert lease.revoked.is_set()
        assert not lease.lost.is_set()
    finally:
        release.set()
        await old_tick
        await keeper.unregister()


class _RecordingTransport(Transport):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[dict[str, object]] = []

    def capture_envelope(self, envelope: Envelope) -> None:
        self.events.extend(item.payload.json for item in envelope.items if item.type == "event" and item.payload.json)


@pytest.fixture
def sentry_events(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[dict[str, object]]]:
    transport = _RecordingTransport()
    monkeypatch.setattr(
        runner,
        "configure_observability",
        lambda: sentry_sdk.init(
            dsn="https://public@example.com/1",
            transport=transport,
            integrations=[LoggingIntegration(event_level=None)],
        ),
    )
    yield transport.events
    sentry_sdk.init()


def _run_main_with(monkeypatch: pytest.MonkeyPatch, run_main: Callable[[str], Awaitable[None]]) -> str:
    dispatch_id = str(uuid4())
    monkeypatch.setattr(runner, "_run_main", run_main)
    monkeypatch.setattr(sys, "argv", ["runner", "--dispatch-id", dispatch_id])
    with pytest.raises(SystemExit):
        runner.main()
    return dispatch_id


def test_runner_failure_before_claim_is_captured_once(
    monkeypatch: pytest.MonkeyPatch, sentry_events: list[dict[str, object]]
) -> None:
    async def fail_claim(_dispatch_id: str) -> None:
        raise RuntimeError("claim failed")

    dispatch_id = _run_main_with(monkeypatch, fail_claim)

    assert len(sentry_events) == 1
    assert cast(dict[str, str], sentry_events[0]["tags"])["executor_dispatch_id"] == dispatch_id


def test_runner_failure_reported_by_dispatch_is_not_captured_again(
    monkeypatch: pytest.MonkeyPatch, sentry_events: list[dict[str, object]]
) -> None:
    async def fail_dispatch(_dispatch_id: str) -> None:
        error = RuntimeError("dispatch failed")
        runner.capture_dispatch_error(error, {"request_id": "request-abc", "trace_headers": {}})
        raise error

    _run_main_with(monkeypatch, fail_dispatch)

    assert len(sentry_events) == 1


def test_runner_failure_is_captured_when_dispatch_telemetry_fails(
    monkeypatch: pytest.MonkeyPatch, sentry_events: list[dict[str, object]]
) -> None:
    def fail_trace(*args: object, **kwargs: object) -> None:
        raise RuntimeError("trace failed")

    monkeypatch.setattr(sentry_sdk, "continue_trace", fail_trace)

    async def fail_dispatch(_dispatch_id: str) -> None:
        error = RuntimeError("dispatch failed")
        runner.capture_dispatch_error(error, {"request_id": "request-abc", "trace_headers": {}})
        raise error

    dispatch_id = _run_main_with(monkeypatch, fail_dispatch)

    assert len(sentry_events) == 1
    assert cast(dict[str, str], sentry_events[0]["tags"])["executor_dispatch_id"] == dispatch_id


@pytest.mark.asyncio
async def test_source_release_creates_cache_runs_checkout_entrypoint_and_rejects_wrong_root(tmp_path: Path) -> None:
    from executor_protocol import source_executor_artifact_uri

    root = tmp_path / "source"
    module = root / "tracker" / "executor"
    module.mkdir(parents=True)
    (module.parent / "__init__.py").touch()
    (module / "__init__.py").touch()
    (module / "entrypoint.py").write_text(
        "import json, sys\n"
        "from pathlib import Path\n"
        "payload = json.loads(Path(sys.argv[1]).read_text())\n"
        "Path(payload['output']).write_text(payload['benchmark_id_str'])\n"
    )
    output = tmp_path / "result.txt"
    dispatch = runner.ArtifactDispatch(
        release_id="source-release",
        artifact_uri=source_executor_artifact_uri(root),
        artifact_digest="a" * 64,
        protocol_version="4",
    )
    supervisor = runner.ExecutorSupervisor(tmp_path / "missing-cache", source_root=root)
    artifact = await supervisor.prepare_artifact(dispatch)
    assert artifact == root
    with pytest.raises(ValueError, match="configured source root"):
        await runner.ExecutorSupervisor(tmp_path, source_root=tmp_path).prepare_artifact(dispatch)

    await supervisor.run(
        artifact,
        dispatch,
        process_payload=runner.ExecutorProcessPayload(
            benchmark_id="benchmark-source",
            verified_task_ids=["task-0"],
            arguments={"benchmark_id_str": "benchmark-source", "output": str(output)},
        ),
        authority=runner.DispatchAuthority("dispatch-source", "benchmark-source"),
        lease=runner._DispatchLease(asyncio.get_running_loop().time(), asyncio.Event(), asyncio.Event()),
    )
    assert output.read_text() == "benchmark-source"
