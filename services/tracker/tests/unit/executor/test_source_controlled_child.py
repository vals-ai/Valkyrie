"""Generation deadline proof at the controlled sandbox boundary."""

from __future__ import annotations

import json
import os
import sys
import subprocess
import textwrap
from pathlib import Path

import benchmark_service
import pytest


# The child calls run_agent with a controlled sandbox at the external service boundary.
_CHILD_PROBE = """
import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace

from benchmark_service import ExecResult, GenerationContainment
from tracker.database.models import AgentContractRequest
from tracker.external_service_gateway import ExternalServiceDeadlineController
from tracker.generation_plan import GenerationPlan
from tracker.sandbox import run_agent


class Workload:
    def __init__(self):
        self.closed = asyncio.Event()
        self.kills = 0
        self.agent_absence_confirmed_at = None

    async def output(self):
        yield "generation started"
        await self.closed.wait()

    async def wait(self):
        await self.closed.wait()
        return SimpleNamespace(
            result=ExecResult(exit_code=0),
            absence_confirmed_at=self.agent_absence_confirmed_at,
        )

    async def kill(self):
        self.kills += 1
        self.agent_absence_confirmed_at = asyncio.get_running_loop().time()
        self.closed.set()


class Sandbox:
    id = "local-controlled"
    name = "local-controlled"
    state = "started"
    generation_containment = GenerationContainment(type="linux_cgroup_v2", version=1)

    def __init__(self):
        self.workload = Workload()
        self.probes = 0
        self.commands = []

    async def probe_generation_containment(self):
        self.probes += 1

    async def exec(self, command):
        self.commands.append(command)
        return ExecResult(exit_code=0)

    def controlled_workload(self, command, *, cwd=None, env_vars):
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
        plan=GenerationPlan(allowance_seconds=0.05, staged=False, episode=None),
        deadline_controller=ExternalServiceDeadlineController(base_allowance_seconds=0.05),
    )
    Path(os.environ["CONTROLLED_PROBE_RESULT"]).write_text(json.dumps({
        "benchmark_id": benchmark_id_str,
        "dispatch_id": executor_dispatch_id,
        "reason": reason.value if reason is not None else None,
        "duration": duration,
        "kills": sandbox.workload.kills,
        "probes": sandbox.probes,
        "absence_confirmed": sandbox.workload.agent_absence_confirmed_at is not None,
        "controlled_command": sandbox.commands[-1],
        "messages": messages,
    }))

"""


def test_run_agent_child_enforces_selected_generation_deadline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_root = Path(__file__).resolve().parents[3] / "src"
    probe_dir = tmp_path / "child-probe"
    probe_dir.mkdir()
    (probe_dir / "sitecustomize.py").write_text(textwrap.dedent(_CHILD_PROBE))
    result_path = tmp_path / "controlled-result.json"
    monkeypatch.setenv("CONTROLLED_PROBE_RESULT", str(result_path))
    benchmark_service_src = Path(benchmark_service.__file__).resolve().parent.parent
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join((str(probe_dir), str(source_root), str(benchmark_service_src))))
    monkeypatch.setenv("SENTRY_DSN", "")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import asyncio; from sitecustomize import controlled_probe; "
            "asyncio.run(controlled_probe(benchmark_id_str='benchmark-source-controlled', "
            "executor_dispatch_id='dispatch-source-controlled'))",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    observed = json.loads(result_path.read_text())
    assert observed["benchmark_id"] == "benchmark-source-controlled"
    assert observed["dispatch_id"] == "dispatch-source-controlled"
    assert observed["reason"] == "TIMEOUT"
    assert observed["kills"] == 1
    assert observed["probes"] == 1
    assert observed["absence_confirmed"] is True
    assert observed["duration"] >= 0.05
    assert "PYTHONSAFEPATH=1 echo task" in observed["controlled_command"]
    assert "timeout " not in observed["controlled_command"]
