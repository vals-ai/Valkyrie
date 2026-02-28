"""Stateless bash tool for agent use in sandbox environments."""

import json
import logging
import subprocess
from typing import Any

from model_library.agent.tool import Tool, ToolOutput


class BashTool(Tool):
    """
    Run a bash command in a sandbox environment.

    Each call is a new subprocess — working directory does NOT persist between
    calls. All commands run from the fixed working_dir. Use absolute paths or
    chain commands with && for multi-step operations.
    """

    def __init__(self, working_dir: str = "/workspace", timeout: int = 120):
        self._working_dir = working_dir
        self._timeout = timeout
        super().__init__(
            name="bash",
            description=(
                f"Run a bash command. "
                f"All commands execute from {working_dir}; the working directory "
                f"does NOT persist between calls. "
                f"Use absolute paths or chain steps with && for multi-step operations."
            ),
            parameters={
                "command": {
                    "type": "string",
                    "description": "The bash command to execute.",
                }
            },
            required=["command"],
        )

    async def execute(self, args: dict[str, Any], state: dict[str, Any], logger: logging.Logger) -> ToolOutput:
        command = args["command"]
        try:
            result = subprocess.run(
                command,
                shell=True,
                cwd=self._working_dir,
                capture_output=True,
                text=True,
                timeout=self._timeout,
            )
        except subprocess.TimeoutExpired:
            output = json.dumps({"exit_code": -1, "stdout": "", "stderr": f"Command timed out after {self._timeout}s"})
            return ToolOutput(output=output, error=output)
        except Exception as e:
            output = json.dumps({"exit_code": -1, "stdout": "", "stderr": str(e)})
            return ToolOutput(output=output, error=output)

        payload = json.dumps(
            {
                "exit_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        )

        if result.returncode != 0:
            return ToolOutput(output=payload, error=payload)

        return ToolOutput(output=payload)
