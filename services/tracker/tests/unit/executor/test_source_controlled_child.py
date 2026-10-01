"""Container-free source-release child proof at the controlled sandbox boundary."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import textwrap
from pathlib import Path

import pytest

from executor_protocol import source_executor_artifact_uri
from tracker.executor import runner


# The child runs the real executor entrypoint and real run_agent. Its test-only
# orchestration adapter replaces the DB-backed benchmark coordinator; the fake
# sandbox is the external Daytona/CBS boundary. No service or container is used.
_CHILD_PROBE = """
import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace

from benchmark_service import CreditedGeneration, ExecResult
from tracker.database.models import AgentContractRequest, GenerationContainment
from tracker.sandbox import run_agent
import tracker.utils.run_orchestration as orchestration


class Workload:
    def __init__(self):
        self.closed = asyncio.Event()
        self.kills = 0
        self.absence_confirmed_at = None

    async def output(self):
        yield "generation started"
        await self.closed.wait()

    async def wait(self):
        await self.closed.wait()
        return SimpleNamespace(
            result=ExecResult(exit_code=0),
            absence_confirmed_at=self.absence_confirmed_at,
        )

    async def kill(self):
        self.kills += 1
        self.absence_confirmed_at = asyncio.get_running_loop().time()
        self.closed.set()


class Sandbox:
    id = "local-controlled"
    name = "local-controlled"
    state = "started"
    generation_containment = GenerationContainment(type="linux_pid_namespace", version=1)

    def __init__(self):
        self.workload = Workload()
        self.probes = 0
        self.commands = []

    async def probe_generation_containment(self):
        self.probes += 1

    async def exec(self, command):
        self.commands.append(command)
        return ExecResult(exit_code=0)

    def controlled_workload(self, command, *, cwd=None):
        self.commands.append(command)
        return self.workload


async def controlled_probe(*, benchmark_id_str, executor_dispatch_id, **_kwargs):
    sandbox = Sandbox()
    messages = []
    reason, duration = await run_agent(
        sandbox,
        AgentContractRequest(name="local-agent", install_cmd="", run_cmd="echo task"),
        "/workspace/problem.txt",
        "task-0",
        messages.append,
        "/workspace",
        object_store=None,
        agent_timeout=0.05,
        task_credited_generation=CreditedGeneration(allowance_seconds=0.05, stage_protocol=None),
    )
    Path(os.environ["CONTROLLED_PROBE_RESULT"]).write_text(json.dumps({
        "benchmark_id": benchmark_id_str,
        "dispatch_id": executor_dispatch_id,
        "reason": reason.value if reason is not None else None,
        "duration": duration,
        "kills": sandbox.workload.kills,
        "probes": sandbox.probes,
        "absence_confirmed": sandbox.workload.absence_confirmed_at is not None,
        "controlled_command": sandbox.commands[-1],
        "messages": messages,
    }))


orchestration.process_benchmark = controlled_probe
"""


@pytest.mark.asyncio
async def test_source_release_child_enforces_selected_generation_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_root = Path(__file__).resolve().parents[3] / "src"
    dispatch = runner.ArtifactDispatch(
        release_id="local-source-release",
        artifact_uri=source_executor_artifact_uri(source_root),
        artifact_digest="a" * 64,
        protocol_version="4",
    )
    # sitecustomize is child-only, and substitutes only the DB-backed coordinator
    # with a driver of the real controlled task API.
    probe_dir = tmp_path / "child-probe"
    probe_dir.mkdir()
    (probe_dir / "sitecustomize.py").write_text(textwrap.dedent(_CHILD_PROBE))
    result_path = tmp_path / "controlled-result.json"
    monkeypatch.setenv("CONTROLLED_PROBE_RESULT", str(result_path))
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join((str(probe_dir), str(source_root))))
    monkeypatch.setenv("SENTRY_DSN", "")
    supervisor = runner.ExecutorSupervisor(tmp_path / "cache", source_root=source_root, python_executable=sys.executable)
    artifact = await supervisor.prepare_artifact(dispatch)
    assert artifact == source_root

    authority = runner.DispatchAuthority("dispatch-source-controlled", "benchmark-source-controlled")
    payload = runner.ExecutorProcessPayload(
        benchmark_id=authority.benchmark_id,
        verified_task_ids=["task-0"],
        arguments={"benchmark_id_str": authority.benchmark_id, "verified_task_ids": ["task-0"]},
    )
    lease = runner._DispatchLease(asyncio.get_running_loop().time(), asyncio.Event(), asyncio.Event())
    async with asyncio.timeout(15):
        await supervisor.run(artifact, dispatch, process_payload=payload, authority=authority, lease=lease)

    observed = json.loads(result_path.read_text())
    assert observed["benchmark_id"] == authority.benchmark_id
    assert observed["dispatch_id"] == authority.dispatch_id
    assert observed["reason"] == "TIMEOUT"
    assert observed["kills"] == 1
    assert observed["probes"] == 1
    assert observed["absence_confirmed"] is True
    assert observed["duration"] >= 0.05
    assert "PYTHONSAFEPATH=1 echo task" in observed["controlled_command"]
    assert "timeout " not in observed["controlled_command"]
