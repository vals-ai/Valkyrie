import os

from agentic_harness.base.contract import AgentContract


anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
if anthropic_api_key is None:
    raise ValueError("ANTHROPIC_API_KEY is not set in the environment")

ALLOWED_TOOLS = [
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


contract = AgentContract(
    name="claude_code",
    uploads=["setup.sh"],
    setup=["bash setup.sh"],
    command=(
        f'$HOME/.local/bin/claude -p "{{prompt}}" --output-format stream-json --verbose '
        f"--allowedTools {','.join(ALLOWED_TOOLS)}"
    ),
    env={
        "ANTHROPIC_API_KEY": anthropic_api_key,
    },
)
