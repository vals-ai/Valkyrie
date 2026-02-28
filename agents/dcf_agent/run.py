"""DCF agent entry point."""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, "/bundle/dcf_agent")

from model_library_agent.run_agent import run_with_tools  # noqa: E402
from tools import BashTool, StopTool  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the DCF agent on a task.")
    parser.add_argument("problem_statement_path", help="Path to the problem statement file.")
    parser.add_argument("task_id", help="Readable task identifier.")
    parser.add_argument("--model", default="anthropic/claude-opus-4-6", help="Model registry key.")
    args = parser.parse_args()

    problem = Path(args.problem_statement_path).read_text()
    asyncio.run(
        run_with_tools(
            tools=[BashTool(working_dir="/workspace"), StopTool()],
            problem_statement=problem,
            model=args.model,
        )
    )


if __name__ == "__main__":
    main()
