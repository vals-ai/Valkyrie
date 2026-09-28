"""Runner lifecycle and child-process authority behavior."""

from __future__ import annotations

import asyncio
import hashlib
import os
import signal
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from executor_protocol import ExecutorTelemetryContext
from tracker.executor import runner


class FakeStore:
    def __init__(self, claim: runner.ClaimedDispatch | None = None) -> None:
        self.available = claim
        self.terminalized: list[tuple[runner.DispatchAuthority, list[str]]] = []
        self.finished: list[runner.DispatchAuthority] = []
        self.renewals: list[runner.DispatchAuthority] = []

    async def claim(self, dispatch_id: str) -> runner.ClaimedDispatch | None:
        claim, self.available = self.available, None
        return claim

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
    monkeypatch.setattr(runner, "ExecutorSupervisor", lambda _cache: supervisor)
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


@pytest.mark.parametrize("protocol_version", ["1", "2", "3"])
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
    monkeypatch.setattr(runner, "ExecutorSupervisor", lambda _cache: supervisor)
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
