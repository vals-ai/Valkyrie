import os
from pathlib import Path

from dotenv import load_dotenv
from model_library.registry_utils import get_model_names_by_provider

from agentic_harness.contract import BaseAgentContract

load_dotenv()


class ClaudeCodeContract(BaseAgentContract):
    """Claude Code Contract"""

    _ALLOWED_MODELS = get_model_names_by_provider("anthropic")

    _ALLOWED_TOOLS = [
        "Bash",
        "Edit",
        "Write",
        "Read",
        "Glob",
        "Grep",
        "LS",
        "WebFetch",
        "NotebookEdit",
        "NotebookRead",
        "TodoRead",
        "TodoWrite",
        "Agent",
        "Skill",
        "SlashCommand",
        "Task",
        "WebSearch",
    ]

    @property
    def name(self) -> str:
        return "claude_code"

    @property
    def artifacts(self) -> list[str]:
        return ["setup.sh"]

    @property
    def install_cmd(self) -> str:
        return "bash setup.sh"

    @property
    def env(self) -> dict[str, str]:
        return {"ANTHROPIC_API_KEY": os.environ["ANTHROPIC_API_KEY"]}

    @property
    def final_output(self) -> Path | None:
        return None

    @property
    def run_cmd(self) -> str:
        args = [
            "-p {problem_statement}",
            "--verbose",
            "--output-format stream-json",
            f"--allowedTools {' '.join(self._ALLOWED_TOOLS)}",
            "2>&1 </dev/null | tee /logs/claude_code.log",  # save output to log file
        ]

        run_cmd = "claude " + " ".join(args)

        return run_cmd


contract = ClaudeCodeContract
