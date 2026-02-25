from pathlib import Path

from agentic_harness.contract import BaseAgentContract


class DCFAgentContract(BaseAgentContract):
    @property
    def name(self) -> str:
        return "dcf_agent"

    @property
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        model = self._agent_config.model
        assert model is not None, "Model must be specified in AgentConfig for DCF Agent"
        return model.query("{{problem_statement}}")

    @property
    def final_output(self) -> Path:
        # TODO: inherit this from problem definition
        return Path("/workspace/financial_statement.xlsx")
