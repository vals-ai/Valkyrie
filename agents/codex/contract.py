from pathlib import Path
from typing import Any, override

from valkyrie.contract import BaseAgentContract


class CodexContract(BaseAgentContract):
    """Codex CLI Contract"""

    _ALLOWED_MODELS = [
        "gpt-5.3-codex",
        "gpt-5.4",
        "gpt-5.2-codex",
        "gpt-5.2",
        "gpt-5.1-codex-max",
        "gpt-5.1-codex-mini",
    ]

    @property
    def name(self) -> str:
        return "codex"

    @property
    def install_cmd(self) -> str:
        return "bash setup.sh"

    @property
    def secrets(self) -> dict[str, str]:
        return {"OPENAI_API_KEY": "prodEvalInfraOpenAIKey"}

    @property
    def final_output(self) -> Path | None:
        return Path("/logs")

    @override
    def run_cmd(self, problem_statement_path: str, task_id: str, kwargs: dict[str, Any]) -> str:
        model_name = self._agent_config.model

        if model_name is None:
            model_name = self._ALLOWED_MODELS[0]

        elif model_name not in self._ALLOWED_MODELS:
            raise ValueError(f"Model {model_name} is not supported. Supported models are {self._ALLOWED_MODELS}")

        args = [
            "exec",
            "--dangerously-bypass-approvals-and-sandbox",
            "--skip-git-repo-check",
            f"--model {model_name}",
            "--json",
            "--enable unified_exec",
            "--",
        ]

        run_cmd = f"cat {problem_statement_path} | codex " + " ".join(args) + " 2>&1 | tee /logs/codex.log"

        return run_cmd


contract = CodexContract
