from __future__ import annotations

import asyncio
import hashlib
import json
from json import JSONDecodeError
import sys
from pathlib import Path

import pytest

import services.executor_host.supervisor as supervisor_module
from services.executor_host.supervisor import (  # pyright: ignore[reportMissingImports]
    ArtifactDispatch,
    ExecutorSupervisor,
    parse_s3_uri,
    run_executor_dispatch,
    verify_file_digest,
)


class FakeDispatchStore:
    def __init__(self, *, claim_result: bool = True) -> None:
        self.claim_result = claim_result
        self.claimed: list[tuple[str, str, ArtifactDispatch]] = []
        self.finished: list[tuple[str, bool]] = []

    async def claim(self, dispatch_id: str, benchmark_id: str, dispatch: ArtifactDispatch) -> bool:
        self.claimed.append((dispatch_id, benchmark_id, dispatch))
        return self.claim_result

    async def finish(self, dispatch_id: str, *, succeeded: bool) -> None:
        self.finished.append((dispatch_id, succeeded))


class FakeS3Client:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.calls: list[tuple[str, str, str]] = []

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        self.calls.append((bucket, key, filename))
        Path(filename).write_bytes(self.content)


def _dispatch(*, digest: str) -> ArtifactDispatch:
    return ArtifactDispatch.from_payload(
        {
            "executor_release_id": "release-v2",
            "executor_artifact_uri": "s3://artifacts/executors/v2.pex",
            "executor_artifact_digest": digest,
            "executor_protocol_version": "1",
        }
    )


def test_parse_s3_uri_requires_bucket_and_key() -> None:
    expected_uri = ("bucket", "path/to/executor.pex")
    assert parse_s3_uri("s3://bucket/path/to/executor.pex") == expected_uri

    with pytest.raises(ValueError, match="s3://"):
        parse_s3_uri("https://bucket/path/to/executor.pex")
    with pytest.raises(ValueError, match="key"):
        parse_s3_uri("s3://bucket/")


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
    supervisor = ExecutorSupervisor(cache_dir=tmp_path, s3_client=client)
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


@pytest.mark.asyncio
async def test_run_forwards_dispatch_payload_to_executor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    script = b"""import json, os, sys\nfrom pathlib import Path\npayload = json.loads(Path(sys.argv[1]).read_text())\nPath(os.environ["EXECUTOR_TEST_MARKER"]).write_text(json.dumps(payload))\n"""
    digest = hashlib.sha256(script).hexdigest()
    marker = tmp_path / "marker.json"
    monkeypatch.setenv("EXECUTOR_TEST_MARKER", str(marker))
    supervisor = ExecutorSupervisor(
        cache_dir=tmp_path,
        s3_client=FakeS3Client(script),
        python_executable=sys.executable,
    )

    await supervisor.run(
        _dispatch(digest=digest),
        start_benchmark_request_json={"benchmark_name": "swebench"},
        benchmark_id_str="benchmark-1",
        verified_task_ids=["task-1"],
    )

    try:
        payload = json.loads(marker.read_text())
    except (OSError, JSONDecodeError) as error:
        raise AssertionError(f"executor marker was not valid JSON: {error}") from error
    assert payload["benchmark_id_str"] == "benchmark-1"
    assert payload["verified_task_ids"] == ["task-1"]
    assert payload["executor_release_id"] == "release-v2"
    assert "executor_dispatch_id" not in payload


@pytest.mark.asyncio
async def test_dispatch_lifecycle_is_owned_by_stable_host(tmp_path: Path) -> None:
    script = b"print('ok')"
    digest = hashlib.sha256(script).hexdigest()
    store = FakeDispatchStore()
    supervisor = ExecutorSupervisor(
        cache_dir=tmp_path,
        s3_client=FakeS3Client(script),
        python_executable=sys.executable,
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

    assert store.claimed == [("dispatch-1", "benchmark-1", _dispatch(digest=digest))]
    assert store.finished == [("dispatch-1", True)]


@pytest.mark.asyncio
async def test_duplicate_dispatch_does_not_launch_executor(tmp_path: Path) -> None:
    store = FakeDispatchStore(claim_result=False)
    supervisor = ExecutorSupervisor(cache_dir=tmp_path, s3_client=FakeS3Client(b"unused"))

    await run_executor_dispatch(
        supervisor,
        store,
        executor_dispatch_id="dispatch-1",
        dispatch=_dispatch(digest="0" * 64),
        start_benchmark_request_json={},
        benchmark_id_str="benchmark-1",
        verified_task_ids=[],
    )

    assert len(store.claimed) == 1
    assert store.finished == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
async def test_failed_executor_terminalizes_dispatch(tmp_path: Path) -> None:
    script = b"raise SystemExit(2)"
    digest = hashlib.sha256(script).hexdigest()
    store = FakeDispatchStore()
    supervisor = ExecutorSupervisor(
        cache_dir=tmp_path,
        s3_client=FakeS3Client(script),
        python_executable=sys.executable,
    )

    with pytest.raises(RuntimeError, match="exited with status 2"):
        await run_executor_dispatch(
            supervisor,
            store,
            executor_dispatch_id="dispatch-1",
            dispatch=_dispatch(digest=digest),
            start_benchmark_request_json={},
            benchmark_id_str="benchmark-1",
            verified_task_ids=[],
        )

    assert store.finished == [("dispatch-1", False)]


@pytest.mark.asyncio
async def test_legacy_dispatch_without_id_still_runs(tmp_path: Path) -> None:
    script = b"print('legacy')"
    digest = hashlib.sha256(script).hexdigest()
    store = FakeDispatchStore()
    supervisor = ExecutorSupervisor(
        cache_dir=tmp_path,
        s3_client=FakeS3Client(script),
        python_executable=sys.executable,
    )

    await run_executor_dispatch(
        supervisor,
        store,
        executor_dispatch_id=None,
        dispatch=_dispatch(digest=digest),
        start_benchmark_request_json={},
        benchmark_id_str="benchmark-1",
        verified_task_ids=[],
    )

    assert store.claimed == []
    assert store.finished == []


@pytest.mark.asyncio
async def test_run_protects_executor_task_while_process_is_running(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    script = b"import time; time.sleep(0.01)"
    digest = hashlib.sha256(script).hexdigest()
    protection_changes: list[bool] = []

    async def record_protection(*, enabled: bool) -> None:
        protection_changes.append(enabled)

    monkeypatch.setattr(supervisor_module, "_set_task_protection", record_protection)
    supervisor = ExecutorSupervisor(
        cache_dir=tmp_path,
        s3_client=FakeS3Client(script),
        python_executable=sys.executable,
    )

    await supervisor.run(
        _dispatch(digest=digest),
        start_benchmark_request_json={},
        benchmark_id_str="benchmark-1",
        verified_task_ids=[],
    )

    assert protection_changes == [True, False]


async def test_task_protection_spans_concurrent_executor_processes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    script = b"import time; time.sleep(0.03)"
    digest = hashlib.sha256(script).hexdigest()
    protection_changes: list[bool] = []

    async def record_protection(*, enabled: bool) -> None:
        protection_changes.append(enabled)

    monkeypatch.setattr(supervisor_module, "_set_task_protection", record_protection)
    supervisor = ExecutorSupervisor(
        cache_dir=tmp_path,
        s3_client=FakeS3Client(script),
        python_executable=sys.executable,
    )

    await asyncio.gather(
        supervisor.run(
            _dispatch(digest=digest),
            start_benchmark_request_json={},
            benchmark_id_str="benchmark-1",
            verified_task_ids=[],
        ),
        supervisor.run(
            _dispatch(digest=digest),
            start_benchmark_request_json={},
            benchmark_id_str="benchmark-2",
            verified_task_ids=[],
        ),
    )

    assert protection_changes == [True, False]


async def test_prepare_artifact_rejects_download_digest_mismatch(tmp_path: Path) -> None:
    client = FakeS3Client(b"wrong content")
    supervisor = ExecutorSupervisor(cache_dir=tmp_path, s3_client=client)

    with pytest.raises(ValueError, match="digest mismatch"):
        await supervisor.prepare_artifact(_dispatch(digest="0" * 64))

    assert list(tmp_path.iterdir()) == []
