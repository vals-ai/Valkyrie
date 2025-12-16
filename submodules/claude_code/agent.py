from model_library.base import InputItem, TextInput
import subprocess
from typing import Any
import json

class ClaudeCodeAgent:
    """
    Runs the first input item through claude code and returns the output.

    NOTE: Assumes all dependencies are correctly installed and credientials are configured.
    """
    async def run(self, input_items: list[InputItem]) -> dict[str, Any]:

        if not isinstance(input_items[0], TextInput):
            raise ValueError("First input item must be a TextInput")

        text_input = input_items[0]

        subprocess_result = subprocess.run(["claude", "-p", f'"{text_input.text}"', "--output-format", "json"], capture_output=True, text=True)

        if subprocess_result.returncode != 0 or subprocess_result.stderr:
            raise ValueError(f"Failed to run claude code: {subprocess_result.stderr}")

        try:
            return json.loads(subprocess_result.stdout)
        except json.JSONDecodeError:
            raise ValueError(f"Failed to parse claude code output: {subprocess_result.stdout}")

