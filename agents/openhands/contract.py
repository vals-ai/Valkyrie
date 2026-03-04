from pathlib import Path
from typing import Any, override

from agentic_harness.contract import BaseAgentContract


class OpenHandsContract(BaseAgentContract):
    """OpenHands Contract"""

    @property
    def name(self) -> str:
        return "openhands"

    @property
    def artifacts(self) -> list[str]:
        return ["setup.sh", "run_with_task_file.py", "base_openhands_config.toml"]

    @property
    def install_cmd(self) -> str:
        return "bash setup.sh"

    @property
    def secrets(self) -> dict[str, str]:
        return {"ANTHROPIC_API_KEY": "devEvalInfraAnthropicKey"}

    @property
    def final_output(self) -> Path | None:
        return Path("/logs/openhands")

    @override
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        model_name = self._agent_config.model
        if not model_name:
            raise ValueError("Model is required. Use --model to specify one.")

        args = [
            f"/bundle/openhands/.venv/bin/python /bundle/openhands/run_with_task_file.py {problem_statement_path}",
            f"--model {model_name}",
            f"--task-id {task_id}",
        ]

        for key, value in kwargs.items():
            args.append(f"--{key} {value}")

        return " ".join(args)


contract = OpenHandsContract
