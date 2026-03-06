from pathlib import Path
from typing import Any, override

from agentic_harness.contract import BaseAgentContract


class ModelLibraryAgentContract(BaseAgentContract):
    @property
    def name(self) -> str:
        return "model_library_agent"

    @property
    def artifacts(self) -> list[str]:
        return ["setup.sh", "model_proxy", "tools"]

    @property
    def install_cmd(self) -> str:
        return "bash setup.sh"

    @property
    def secrets(self) -> dict[str, str]:
        return {
            "ANTHROPIC_API_KEY": "devEvalInfraAnthropicKey",
            "OPENAI_API_KEY": "devEvalInfraOpenAIKey",
            "GOOGLE_API_KEY": "devEvalInfraGoogleKey",
        }

    @property
    def final_output(self) -> Path:
        return Path("/workspace")

    @override
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        model = self._agent_config.model
        if not model:
            raise ValueError("Model is required. Use --model to specify one.")

        # hardcode tools for now
        tools = "bash,stop,recalculate_excel"

        args = [
            "PYTHONPATH=/bundle/model_library_agent",
            "python -m model_library.agent.cli",
            f"--model {model}",
            f"--problem-statement {problem_statement_path}",
            f"--tools {tools}",
            "--tool-module tools",
            "--log-file /logs/agent.log",
            "--console",
            "--output /workspace/result.json",
        ]

        return " ".join(args)


contract = ModelLibraryAgentContract
