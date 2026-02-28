import os
from pathlib import Path
from typing import Any

from agentic_harness.contract import BaseAgentContract


class DCFAgentContract(BaseAgentContract):
    @property
    def name(self) -> str:
        return "dcf_agent"

    @property
    def artifacts(self) -> list[str]:
        return ["run.py", "requirements.txt"]

    @property
    def shared_artifacts(self) -> list[str]:
        return ["model_library_agent", "tools"]

    @property
    def install_cmd(self) -> str:
        return "pip install -r requirements.txt"

    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        if not self._agent_config.model:
            raise ValueError("model is required for DCFAgentContract")
        model = self._agent_config.model
        return (
            f"python /bundle/dcf_agent/run.py {problem_statement_path} {task_id}"
            f" --model {model} 2>&1 | tee /tmp/dcf_agent.log"
        )

    @property
    def env(self) -> dict[str, str]:
        return {"ANTHROPIC_API_KEY": os.environ["ANTHROPIC_API_KEY"]}

    @property
    def final_output(self) -> Path:
        return Path("/workspace/template.xlsx")


contract = DCFAgentContract
