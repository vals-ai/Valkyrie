"""Stop, resume, or retry an existing Valkyrie run.

Run from the repository root:

    uv run python docs/sdk/examples/manage_run.py stop RUN_ID
    uv run python docs/sdk/examples/manage_run.py resume RUN_ID --concurrency 10
    uv run python docs/sdk/examples/manage_run.py retry RUN_ID --task-id TASK_ID
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from uuid import UUID

from valkyrie.sdk import ValkyrieClient, ValkyrieSDKError


def add_retry_options(parser: argparse.ArgumentParser) -> None:
    """Add options shared by resume and retry operations."""
    parser.add_argument("run_id", type=UUID, help="Run UUID")
    parser.add_argument("--concurrency", type=int, help="Override concurrent tasks")
    parser.add_argument("--task-id", action="append", dest="task_ids", help="Task ID; repeat for multiple tasks")
    parser.add_argument("--from-scratch", action="store_true", help="Restart selected work from scratch")


def parse_args() -> argparse.Namespace:
    """Parse one explicit run-management action."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    actions = parser.add_subparsers(dest="action", required=True)

    stop_parser = actions.add_parser("stop", help="Stop a running run")
    stop_parser.add_argument("run_id", type=UUID, help="Run UUID")
    stop_parser.add_argument("--force", action="store_true", help="Terminate active sandboxes")

    add_retry_options(actions.add_parser("resume", help="Resume unfinished work"))
    add_retry_options(actions.add_parser("retry", help="Retry failed or selected work"))
    return parser.parse_args()


async def manage_run(args: argparse.Namespace) -> None:
    """Fetch a run and perform the selected management action."""
    async with ValkyrieClient.from_config() as client:
        current = await client.runs.fetch(args.run_id)
        print(f"{current.benchmark_id} is {current.details.status.value}")

        if args.action == "stop":
            response = await client.runs.stop(args.run_id, force=args.force)
        elif args.action == "resume":
            response = await client.runs.resume(
                args.run_id,
                concurrency=args.concurrency,
                task_ids=args.task_ids,
                from_scratch=args.from_scratch,
            )
        else:
            response = await client.runs.retry(
                args.run_id,
                concurrency=args.concurrency,
                task_ids=args.task_ids,
                from_scratch=args.from_scratch,
            )

        print(f"{args.action}: {response.status}")


async def main(args: argparse.Namespace) -> int:
    """Run the example and convert SDK errors into a concise CLI failure."""
    try:
        await manage_run(args)
    except ValkyrieSDKError as exc:
        print(f"Valkyrie SDK error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(parse_args())))
