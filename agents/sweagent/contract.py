from pathlib import Path
from typing import Any, override

from agentic_harness.contract import BaseAgentContract


class SWEAgentContract(BaseAgentContract):
    """SWE Agent Contract"""

    @property
    def name(self) -> str:
        return "sweagent"

    @property
    def artifacts(self) -> list[str]:
        return ["setup.sh", "sweagent"]

    @property
    def install_cmd(self) -> str:
        return "bash setup.sh"

    @property
    def final_output(self) -> Path | None:
        return Path("/logs/sweagent")

    @override
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        model_name = self._agent_config.model

        if not model_name:
            raise ValueError(
                "Model key not detected, model is required to run sweagent. Use --model to assign a model to run"
            )

        args = [
            "--env.deployment.type=local",
            "--env.repo.type=preexisting",
            "--env.repo.repo_name=testbed",
            "--agent.model.provider=vals",
            f"--problem_statement.path={problem_statement_path}",
            "--config=/bundle/sweagent/sweagent/config/default.yaml",
            "--output_dir=/logs/sweagent",
            f"--agent.model.name={model_name}",
        ]

        run_cmd = "sweagent run " + " ".join(args)

        return run_cmd


contract = SWEAgentContract
