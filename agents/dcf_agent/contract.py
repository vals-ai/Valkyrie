from pathlib import Path
from typing import Any

from agentic_harness.contract import BaseAgentContract


class DCFAgentContract(BaseAgentContract):
    @property
    def name(self) -> str:
        return "dcf_agent"

    @property
    def artifacts(self) -> list[str]:
        return ["requirements.txt"]

    @property
    def install_cmd(self) -> str:
        return "pip install -r requirements.txt"  # falsy string to skip

    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        return f"echo '=== Problem Statement ===' && cat {problem_statement_path} && echo '=== Workspace Files ===' && find /workspace -type f | sort"

    @property
    def final_output(self) -> Path:
        # TODO: inherit this from problem definition
        return Path("/workspace/template.xlsx")


contract = DCFAgentContract
