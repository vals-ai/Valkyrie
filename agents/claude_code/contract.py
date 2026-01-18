import os
from dotenv import load_dotenv

from agentic_harness.contract import BaseAgentContract

load_dotenv()


class ClaudeCodeContract(BaseAgentContract):
    """Claude Code Contract"""

    @property
    def name(self) -> str:
        return "claude_code"

    @property
    def artifacts(self) -> list[str]:
        return ["setup.sh", "submodules/claude_code"]

    @property
    def install_cmd(self) -> str:
        return "bash setup.sh"

    @property
    def env(self) -> dict[str, str]:
        return {"ANTHROPIC_API_KEY": os.environ["ANTHROPIC_API_KEY"]}

    @property
    def run_cmd(self) -> str:
        return "claude_code -p {{problem_statement}}"


contract = ClaudeCodeContract
