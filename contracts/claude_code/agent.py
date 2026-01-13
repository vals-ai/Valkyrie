import asyncio
import json
import os
from typing import Any, AsyncGenerator, TypeAlias

from agentic_harness.base.types import InputItem, TextInput

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | dict[str, "JSONValue"] | list["JSONValue"]


class ClaudeCodeAgent:
    """
    Runs the first input item through claude code and returns the output.

    NOTE: Assumes all dependencies are correctly installed and credientials are configured.
    """

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
    ]

    async def _stream_conversation(self, stream: asyncio.StreamReader) -> AsyncGenerator[str, Any]:
        while True:
            line = await stream.readline()
            if not line:
                break

            yield line.decode().strip()

    def _parse_sequence(self, sequence: str) -> dict[str, Any] | None:
        try:
            raw_data = json.loads(sequence)

            return raw_data
        except json.JSONDecodeError:
            return None

    def walk_paths(self, obj: JSONValue, path: str = "") -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                next_path = f"{path}.{k}" if path else k
                self.walk_paths(v, next_path)

        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                self.walk_paths(item, f"{path}[{i}]")

        else:
            print(f"{path} = {obj!r}", flush=True)

    async def _read_stderr(self, stderr: asyncio.StreamReader) -> str:
        data = await stderr.read()
        return data.decode()

    async def run(self, input_items: list[InputItem]) -> dict[str, Any]:
        if not isinstance(input_items[0], TextInput):
            raise ValueError("First input item must be a TextInput")

        text_input = input_items[0]

        # Stream each turn from the conversation, that way we can log it as it comes in
        subprocess_result = await asyncio.create_subprocess_exec(
            "claude",
            "-p",
            text_input.text,
            "--output-format",
            "stream-json",
            "--verbose",
            "--allowedTools",
            f"{''.join(self._ALLOWED_TOOLS)}",
            stdin=asyncio.subprocess.DEVNULL,  # NOTE: Daytona does not provide a stdin when executed from command externally
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ, "PATH": f"{os.environ['HOME']}/.local/bin:{os.environ['PATH']}"},
        )

        if not subprocess_result.stdout or not subprocess_result.stderr:
            raise ValueError("Subprocess did not produce a stdout or stderr stream")

        stderr_task = asyncio.create_task(self._read_stderr(subprocess_result.stderr))

        result_sequence: dict[str, Any] = {}
        async for line in self._stream_conversation(subprocess_result.stdout):
            parsed_line = self._parse_sequence(line)

            if parsed_line:
                self.walk_paths(parsed_line)

            if parsed_line and parsed_line.get("type") == "result":
                result_sequence = parsed_line

        stderr_output = await stderr_task
        await subprocess_result.wait()

        if subprocess_result.returncode != 0:
            raise ValueError(
                f"Subprocess failed with return code: {subprocess_result.returncode}. stderr: {str(stderr_output)}"
            )

        if not result_sequence:
            raise ValueError("No result sequence found")

        return result_sequence
