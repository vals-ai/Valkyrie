"""Native output HTTP, persistence, and sandbox-boundary tests.

Run: .venv/bin/pytest tests/integration/local/api/test_native_output.py -q
"""

import asyncio
import shutil
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from benchmark_service import ComposeSource, ImageSource, SandboxError
from benchmark_service.sandbox.types import ExecResult, SandboxNotFoundError
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark, make_task
from tracker.api.dependencies import get_run_runtime
from tracker.database.models import ActiveNativeOutput, Benchmark, Org, Task, TaskStatus

_STARTED_AT = datetime(2026, 7, 1, tzinfo=UTC)
_HEADERS = {"X-Api-Key": "fake"}


@dataclass
class NativeCase:
    benchmark: Benchmark
    task: Task
    root: Path
    runtime: MagicMock
    provider: MagicMock
    sandbox: MagicMock

    @property
    def url(self) -> str:
        return f"/benchmarks/{self.benchmark.id}/tasks/{self.task.task_id}/native-output"

    @property
    def descriptor(self) -> ActiveNativeOutput:
        assert self.task.active_native_output is not None

        return self.task.active_native_output


@pytest.fixture
def native_case(
    local_app: FastAPI,
    database_session: Session,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> NativeCase:
    benchmark = make_benchmark(session=database_session)
    task = make_task(benchmark, "native-task", status=TaskStatus.IN_PROGRESS, started_at=_STARTED_AT)
    root = tmp_path / "native output"
    root.mkdir()
    task.active_native_output = ActiveNativeOutput(
        attempt_started_at=_STARTED_AT,
        sandbox_id="sandbox-current",
        runtime_source=ImageSource(image="agent-image"),
        output_path=str(root),
    )
    database_session.add(task)
    database_session.commit()
    database_session.refresh(task)

    async def execute(command: str, *, timeout: float) -> ExecResult:
        process = await asyncio.create_subprocess_exec(
            "sh", "-c", command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        output, error = await asyncio.wait_for(process.communicate(), timeout=timeout)
        assert process.returncode is not None

        return ExecResult(exit_code=process.returncode, output=(output + error).decode())

    async def download(remote_path: str) -> bytes:
        # Modal's filesystem API requires absolute paths, unlike shell execution.
        assert Path(remote_path).is_absolute()
        return Path(remote_path).read_bytes()

    sandbox = MagicMock()
    sandbox.exec = AsyncMock(side_effect=execute)
    sandbox.download_file = AsyncMock(side_effect=download)
    provider = MagicMock()
    provider.get_sandbox = AsyncMock(return_value=sandbox)
    config = MagicMock()
    config.create_provider.return_value.__aenter__.return_value = provider
    runtime = MagicMock()
    runtime.get_sandbox_provider_config = AsyncMock(return_value=config)
    monkeypatch.setitem(local_app.dependency_overrides, get_run_runtime, lambda: runtime)

    return NativeCase(benchmark, task, root, runtime, provider, sandbox)


class TestNativeOutput:
    """Authorized direct native reads and complete generation fencing."""

    @pytest.mark.parametrize("status", [TaskStatus.BUILDING, TaskStatus.IN_PROGRESS, TaskStatus.EVALUATING])
    @pytest.mark.parametrize("single_file", [False, True])
    def test_reads_unchanged_bytes_without_object_storage(
        self,
        client: TestClient,
        database_session: Session,
        native_case: NativeCase,
        status: TaskStatus,
        single_file: bool,
    ) -> None:
        native_case.task.status = status
        database_session.add(native_case.task)
        database_session.commit()
        file = native_case.root / "agent output.log"
        content = b"native\x00bytes\nnot JSON\xff"
        file.write_bytes(content)
        if single_file:
            native_case.task.active_native_output = native_case.descriptor.model_copy(update={"output_path": str(file)})
            database_session.add(native_case.task)
            database_session.commit()

        listing = client.get(f"{native_case.url}/files", headers=_HEADERS)

        assert listing.status_code == 200, listing.text
        assert listing.json()["files"] == [{"path": file.name, "size_bytes": len(content)}]
        generation = listing.json()["generation"]
        assert generation == native_case.descriptor.generation

        response = client.get(
            f"{native_case.url}/file", params={"path": file.name, "generation": generation}, headers=_HEADERS
        )

        assert response.status_code == 200, response.text
        assert response.content == content
        assert response.headers["x-native-output-generation"] == generation
        assert response.headers["cache-control"] == listing.headers["cache-control"] == "no-store"
        native_case.provider.get_sandbox.assert_awaited_with("sandbox-current")
        native_case.sandbox.download_file.assert_awaited_once_with(str(file))
        assert native_case.runtime.objects.mock_calls == []

        detail = client.get(native_case.url.removesuffix("/native-output"), headers=_HEADERS)

        assert "active_native_output" not in detail.json()
        assert "active_native_output" not in native_case.task.model_dump()

    def test_nested_files_exclude_symlinks(self, client: TestClient, native_case: NativeCase, tmp_path: Path) -> None:
        nested = native_case.root / "nested"
        nested.mkdir()
        (nested / "turns\nwith space.jsonl").write_bytes(b"{}\n")
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "secret").write_text("not allowed")
        (native_case.root / "linked-dir").symlink_to(outside, target_is_directory=True)
        (native_case.root / "linked-file").symlink_to(outside / "secret")

        listing = client.get(f"{native_case.url}/files", headers=_HEADERS)

        assert listing.status_code == 200, listing.text
        assert listing.json()["files"] == [{"path": "nested/turns\nwith space.jsonl", "size_bytes": 3}]
        for path in ("linked-dir/secret", "linked-file", "missing"):
            response = client.get(
                f"{native_case.url}/file",
                params={"path": path, "generation": listing.json()["generation"]},
                headers=_HEADERS,
            )
            assert response.status_code == 404
        native_case.sandbox.download_file.assert_not_awaited()

    @pytest.mark.parametrize("path", ["", "/etc/passwd", "../secret", "a/../b", "a//b", "./a", "a/", "a\\b", "a\x00b"])
    def test_rejects_malformed_paths(self, client: TestClient, native_case: NativeCase, path: str) -> None:
        response = client.get(
            f"{native_case.url}/file",
            params={"path": path, "generation": native_case.descriptor.generation},
            headers=_HEADERS,
        )

        assert response.status_code == 400
        native_case.runtime.get_sandbox_provider_config.assert_not_awaited()

    @pytest.mark.parametrize(
        "state", ["no-descriptor", "old-attempt", "finished", "stopped", "missing-target", "symlink-target"]
    )
    def test_unavailable_has_no_archive_fallback(
        self, client: TestClient, database_session: Session, native_case: NativeCase, state: str, tmp_path: Path
    ) -> None:
        generation = native_case.descriptor.generation
        if state == "no-descriptor":
            native_case.task.active_native_output = None
        elif state == "old-attempt":
            native_case.task.started_at += timedelta(seconds=1)
        elif state in ("finished", "stopped"):
            native_case.task.status = TaskStatus.FINISHED if state == "finished" else TaskStatus.STOPPED
        else:
            target = tmp_path / "target"
            if state == "symlink-target":
                target.symlink_to(native_case.root, target_is_directory=True)
            native_case.task.active_native_output = native_case.descriptor.model_copy(
                update={"output_path": str(target)}
            )
        database_session.add(native_case.task)
        database_session.commit()

        listing = client.get(f"{native_case.url}/files", headers=_HEADERS)
        response = client.get(
            f"{native_case.url}/file", params={"path": "file", "generation": generation}, headers=_HEADERS
        )

        assert listing.status_code == response.status_code == 204
        assert response.content == listing.content == b""
        assert native_case.runtime.objects.mock_calls == []
        if state not in ("missing-target", "symlink-target"):
            native_case.runtime.get_sandbox_provider_config.assert_not_awaited()

    @pytest.mark.parametrize("scope", ["org", "task", "benchmark"])
    def test_scoped_not_found_does_not_access_provider(
        self, client: TestClient, database_session: Session, native_case: NativeCase, scope: str
    ) -> None:
        url = native_case.url
        if scope == "org":
            foreign_org = Org(id=UUID(int=22), name="other")
            database_session.add(foreign_org)
            native_case.benchmark.org_id = foreign_org.id
            database_session.add(native_case.benchmark)
            database_session.commit()
        elif scope == "task":
            url = url.replace("native-task", "missing-task")
        else:
            url = url.replace(str(native_case.benchmark.id), str(UUID(int=33)))

        response = client.get(f"{url}/files", headers=_HEADERS)

        assert response.status_code == 404
        native_case.runtime.get_sandbox_provider_config.assert_not_awaited()

    def test_auth_and_required_queries(self, client: TestClient, native_case: NativeCase) -> None:
        assert client.get(f"{native_case.url}/files").status_code == 401
        assert client.get(f"{native_case.url}/file", headers=_HEADERS).status_code == 422
        assert client.get(f"{native_case.url}/file", params={"path": "a"}, headers=_HEADERS).status_code == 422
        assert client.get(f"{native_case.url}/file", params={"generation": "a"}, headers=_HEADERS).status_code == 422
        native_case.runtime.get_sandbox_provider_config.assert_not_awaited()

    @pytest.mark.parametrize(
        ("after_read", "change"),
        [
            (False, "sandbox"),
            (True, "sandbox"),
            (True, "attempt"),
            (True, "stop"),
            (True, "clear"),
        ],
    )
    def test_generation_change_rejects_stale_bytes(
        self, client: TestClient, database_session: Session, native_case: NativeCase, after_read: bool, change: str
    ) -> None:
        (native_case.root / "trajectory.json").write_bytes(b"old bytes")
        generation = native_case.descriptor.generation
        replacement = native_case.descriptor.model_copy(update={"sandbox_id": "sandbox-replacement"})

        def replace() -> None:
            with Session(database_session.get_bind()) as other_session:
                task = other_session.get(Task, native_case.task.id)
                assert task is not None
                if change == "attempt":
                    task.started_at += timedelta(seconds=1)
                elif change == "stop":
                    task.status = TaskStatus.STOPPED
                else:
                    task.active_native_output = None if change == "clear" else replacement
                other_session.add(task)
                other_session.commit()

        async def download(_path: str) -> bytes:
            replace()

            return b"old bytes"

        if after_read:
            native_case.sandbox.download_file.side_effect = download
        else:
            replace()
            database_session.refresh(native_case.task)

        response = client.get(
            f"{native_case.url}/file", params={"path": "trajectory.json", "generation": generation}, headers=_HEADERS
        )

        assert response.status_code == 409
        assert b"old bytes" not in response.content
        if not after_read:
            native_case.runtime.get_sandbox_provider_config.assert_not_awaited()

    def test_compose_source_roundtrip_selects_original_service(
        self, client: TestClient, database_session: Session, native_case: NativeCase
    ) -> None:
        source = ComposeSource(
            outer=ImageSource(image="outer-image"), service="worker", compose_command="docker compose -f /custom.yml"
        )
        native_case.task.active_native_output = native_case.descriptor.model_copy(update={"runtime_source": source})
        database_session.add(native_case.task)
        database_session.commit()
        database_session.expire_all()
        commands: list[str] = []

        async def execute(command: str, **_kwargs: Any) -> ExecResult:
            commands.append(command)
            if "-T worker sh -lc" in command:
                return ExecResult(exit_code=0, output=f"d\0{native_case.root}/trajectory.json\0{2}\0")

            return ExecResult(exit_code=0)

        native_case.sandbox.exec.side_effect = execute
        native_case.sandbox.download_file.side_effect = None
        native_case.sandbox.download_file.return_value = b"{}"

        response = client.get(
            f"{native_case.url}/file",
            params={"path": "trajectory.json", "generation": native_case.descriptor.generation},
            headers=_HEADERS,
        )

        assert response.status_code == 200, response.text
        assert response.content == b"{}"
        assert native_case.descriptor.runtime_source == source
        assert any(
            "docker compose -f /custom.yml exec" in command and "-T worker sh -lc" in command for command in commands
        )
        assert any("docker compose -f /custom.yml ps -q worker" in command for command in commands)
        assert native_case.runtime.objects.mock_calls == []

    @pytest.mark.parametrize("disappears", [False, True])
    def test_vanished_file_is_omitted_without_hiding_other_metadata_errors(
        self,
        client: TestClient,
        native_case: NativeCase,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        disappears: bool,
    ) -> None:
        changing_file = native_case.root / "changing.log"
        changing_file.write_text("old")
        (native_case.root / "surviving.log").write_text("still here")
        real_ls = shutil.which("ls")
        assert real_ls is not None
        commands = tmp_path / "bin"
        commands.mkdir()
        wrapper = commands / "ls"
        failure = (
            "    os.unlink(sys.argv[-1])\n"
            if disappears
            else "    sys.stderr.write('metadata denied\\n')\n    sys.exit(1)\n"
        )
        wrapper.write_text(
            f"#!{sys.executable}\n"
            "import os, sys\n"
            f"if sys.argv[-1] == {str(changing_file)!r}:\n"
            f"{failure}"
            f"os.execv({real_ls!r}, [{real_ls!r}, *sys.argv[1:]])\n"
        )
        wrapper.chmod(0o700)

        with monkeypatch.context() as metadata_failure:
            metadata_failure.setenv("PATH", str(commands), prepend=":")
            if disappears:
                listing = client.get(f"{native_case.url}/files", headers=_HEADERS)
                assert listing.status_code == 200, listing.text
                assert listing.json()["files"] == [{"path": "surviving.log", "size_bytes": 10}]
                missing = client.get(
                    f"{native_case.url}/file",
                    params={"path": "changing.log", "generation": listing.json()["generation"]},
                    headers=_HEADERS,
                )
                assert missing.status_code == 404
            else:
                with pytest.raises(SandboxError, match="Native output discovery failed"):
                    client.get(f"{native_case.url}/files", headers=_HEADERS)

        if not disappears:
            listing = client.get(f"{native_case.url}/files", headers=_HEADERS)
            assert listing.status_code == 200, listing.text
            assert listing.json()["files"] == [
                {"path": "changing.log", "size_bytes": 3},
                {"path": "surviving.log", "size_bytes": 10},
            ]

        response = client.get(
            f"{native_case.url}/file",
            params={"path": "surviving.log", "generation": listing.json()["generation"]},
            headers=_HEADERS,
        )

        assert response.status_code == 200, response.text
        assert response.content == b"still here"

    @pytest.mark.parametrize("stage", ["attach", "download"])
    def test_provider_errors_are_not_wrapped(self, client: TestClient, native_case: NativeCase, stage: str) -> None:
        error = SandboxError("provider error")
        if stage == "attach":
            native_case.provider.get_sandbox.side_effect = error
        else:
            (native_case.root / "file.log").write_text("content")
            native_case.sandbox.download_file.side_effect = error

        with pytest.raises(SandboxError) as raised:
            client.get(
                f"{native_case.url}/file",
                params={"path": "file.log", "generation": native_case.descriptor.generation},
                headers=_HEADERS,
            )

        assert raised.value is error

    @pytest.mark.parametrize("stage", ["attach", "discover", "download"])
    def test_sandbox_gone_is_unavailable(self, client: TestClient, native_case: NativeCase, stage: str) -> None:
        error = SandboxNotFoundError("Modal Sandbox is shutting down.")
        (native_case.root / "file.log").write_text("content")
        if stage == "attach":
            native_case.provider.get_sandbox.side_effect = error
        elif stage == "discover":
            native_case.sandbox.exec.side_effect = error
        else:
            native_case.sandbox.download_file.side_effect = error

        listing = client.get(f"{native_case.url}/files", headers=_HEADERS)
        response = client.get(
            f"{native_case.url}/file",
            params={"path": "file.log", "generation": native_case.descriptor.generation},
            headers=_HEADERS,
        )

        assert response.status_code == 204
        assert listing.status_code == (200 if stage == "download" else 204)
