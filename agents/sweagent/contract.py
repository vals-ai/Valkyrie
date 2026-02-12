from pathlib import Path

from dotenv import dotenv_values

from agentic_harness.contract import BaseAgentContract


class SWEAgentContract(BaseAgentContract):
    """SWE Agent Contract"""

    @property
    def name(self) -> str:
        return "sweagent"

    @property
    def artifacts(self) -> list[str]:
        return ["setup.sh", "submodules/sweagent"]

    @property
    def install_cmd(self) -> str:
        return "bash setup.sh"

    @property
    def env(self) -> dict[str, str]:
        return {k: v for k, v in dotenv_values().items() if v is not None}

    @property
    def final_output(self) -> Path | None:
        return Path("/logs/sweagent")

    @property
    def run_cmd(self) -> str:
        args = [
            "--env.deployment.type=local",
            "--env.repo.type=preexisting",
            "--env.repo.repo_name=/testbed",
            "--agent.model.provider=vals",
            "--problem_statement.text={problem_statement}",
            "--config=/bundle/sweagent/submodules/sweagent/config/default.yaml",
            "--output_dir=/logs/sweagent",
        ]

        model_name = self._agent_config.model
        if model_name:
            args.append(f"--agent.model.name={model_name}")

        run_cmd = "sweagent run " + " ".join(args)

        return run_cmd


contract = SWEAgentContract
