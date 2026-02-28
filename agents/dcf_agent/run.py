"""DCF agent — self-contained loop using the Anthropic SDK directly."""

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path

import anthropic

logging.basicConfig(level=logging.INFO, stream=sys.stdout, force=True)
log = logging.getLogger(__name__)

TOOLS = [
    {
        "name": "bash",
        "description": (
            "Run a bash command. All commands execute from /workspace; the working "
            "directory does NOT persist between calls. Use absolute paths or chain "
            "steps with && for multi-step operations."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "The bash command to execute."}
            },
            "required": ["command"],
        },
    },
    {
        "name": "stop",
        "description": (
            "Stop the agent. Call this when the task is complete. "
            "You will continue to be prompted until you call this tool."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
]


def run_bash(command: str, timeout: int = 120) -> str:
    try:
        result = subprocess.run(
            command, shell=True, cwd="/workspace", capture_output=True, text=True, timeout=timeout
        )
        return json.dumps({"exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr})
    except subprocess.TimeoutExpired:
        return json.dumps({"exit_code": -1, "stdout": "", "stderr": f"timed out after {timeout}s"})
    except Exception as e:
        return json.dumps({"exit_code": -1, "stdout": "", "stderr": str(e)})


def run_agent(problem_statement: str, model: str, max_turns: int = 100) -> None:
    # model-library uses "anthropic/model-id" — strip provider prefix for SDK
    sdk_model = model.split("/", 1)[-1]
    client = anthropic.Anthropic()
    messages: list[dict] = [{"role": "user", "content": problem_statement}]

    for turn in range(max_turns):
        log.info("Turn %d", turn + 1)
        response = client.messages.create(
            model=sdk_model,
            max_tokens=32000,
            tools=TOOLS,
            messages=messages,
        )
        log.info("stop_reason=%s", response.stop_reason)

        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            log.info("Agent finished (end_turn)")
            break

        # If truncated, tell the model and let it recover
        if response.stop_reason == "max_tokens":
            log.warning("Response truncated (max_tokens); asking model to continue")
            messages.append({"role": "user", "content": "Your last response was truncated. Please continue."})
            continue

        tool_results = []
        done = False
        for block in response.content:
            if block.type != "tool_use":
                continue

            inputs = getattr(block, "input", {}) or {}
            log.info("Tool call: %s %s", block.name, inputs)

            if block.name == "stop":
                done = True
                tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": ""})

            elif block.name == "bash":
                command = inputs.get("command")
                if not command:
                    log.warning("bash tool called with no command, skipping")
                    tool_results.append({"type": "tool_result", "tool_use_id": block.id,
                                         "content": json.dumps({"exit_code": 1, "stdout": "", "stderr": "no command provided"})})
                    continue
                output = run_bash(command)
                parsed = json.loads(output)
                log.info("exit_code=%d stdout=%s", parsed["exit_code"], parsed["stdout"][:500])
                if parsed["stderr"]:
                    log.warning("stderr: %s", parsed["stderr"][:500])
                tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": output})

        if tool_results:
            messages.append({"role": "user", "content": tool_results})

        if done:
            log.info("Agent finished (stop tool)")
            break
    else:
        log.warning("Reached max_turns=%d without stopping", max_turns)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("problem_statement_path")
    parser.add_argument("task_id")
    parser.add_argument("--model", default="anthropic/claude-opus-4-6")
    args = parser.parse_args()

    problem = Path(args.problem_statement_path).read_text()
    run_agent(problem, args.model)


if __name__ == "__main__":
    main()
