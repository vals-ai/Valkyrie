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
    def run_cmd(self) -> str:
        if not self._agent_config:
            raise ValueError("SWEAgentContract requires and AgentConfig")

        model_name = self._agent_config.model

        args = [
            "--env.deployment.type=local",
            "--env.repo.type=preexisting",
            "--env.repo.repo_name=/testbed",
            "--agent.model.provider=vals",
            f"--agent.model.name={model_name}",
            "--problem_statement.text={{problem_statement}}",
            "--config=/bundle/sweagent/submodules/sweagent/config/default.yaml",
            "--output_dir=/logs",
        ]

        run_cmd = "sweagent run " + " ".join(args)

        return run_cmd


contract = SWEAgentContract
