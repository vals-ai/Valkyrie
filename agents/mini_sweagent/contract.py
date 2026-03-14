from pathlib import Path
from typing import Any, override

from valkyrie.contract import BaseAgentContract


class MiniSWEAgentContract(BaseAgentContract):
    """Mini SWE Agent Contract"""

    @property
    def name(self) -> str:
        return "mini_sweagent"

    @property
    def install_cmd(self) -> str:
        return "bash setup.sh"

    @property
    def final_output(self) -> Path | None:
        return Path("/logs/mini_sweagent")

    @override
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        model_name = self._agent_config.model

        if not model_name:
            raise ValueError(
                "Model key not detected, model is required to run mini_sweagent. Use --model to assign a model to run"
            )
        args = [
            f"python3 /bundle/mini_sweagent/run_with_task_file.py {problem_statement_path}",
            "--config=swebench.yaml",
            "--environment-class=local",
            f"--config=model.model_name={model_name}",
            "--output=/logs/mini_sweagent/trajectory.json",
            "--agent-class=default",
            "--exit-immediately",
            "--yolo",
        ]

        for key, value in kwargs.items():
            args.append(f"--config={key}={value}")

        return " ".join(args)


contract = MiniSWEAgentContract
