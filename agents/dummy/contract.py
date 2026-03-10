from pathlib import Path
from typing import Any, override

from agentic_harness.contract import BaseAgentContract


class DummyAgentContract(BaseAgentContract):
    @property
    def name(self) -> str:
        return "dummy"

    @property
    def install_cmd(self) -> str:
        return "echo 'installing dummy agent'"

    @property
    def final_output(self) -> Path | None:
        return None

    @override
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        return "echo 'running dummy agent'"


contract = DummyAgentContract
