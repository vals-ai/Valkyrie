"""Start a Valkyrie run and follow it through result retrieval.

Run from the repository root:

    uv run python docs/sdk/examples/run_lifecycle.py AGENT BENCHMARK --model MODEL
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from valkyrie.sdk import BenchmarkStatus, ValkyrieClient, ValkyrieSDKError


def parse_args() -> argparse.Namespace:
    """Parse the values needed to start a run."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("agent", help="Uploaded agent name")
    parser.add_argument("benchmark", help="Benchmark service name")
    parser.add_argument("--model", help="Model override for the agent")
    parser.add_argument("--dataset", help="Benchmark dataset name")
    parser.add_argument("--label", help="Optional run label")
    parser.add_argument("--provider", help="Configured sandbox provider")
    parser.add_argument("--concurrency", type=int, default=5, help="Concurrent tasks (default: 5)")
    return parser.parse_args()


async def run_lifecycle(args: argparse.Namespace) -> None:
    """Start, observe, and retrieve one run."""
    async with ValkyrieClient.from_config() as client:
        run = await client.runs.start(
            agent=args.agent,
            benchmark=args.benchmark,
            model=args.model,
            dataset=args.dataset,
            label=args.label,
            provider=args.provider,
            concurrency=args.concurrency,
        )
        print(f"Started {run.benchmark_id}")

        async for update in client.runs.stream(run.benchmark_id):
            details = update.details
            print(f"{details.status.value}: {details.finished_tasks}/{details.total_tasks} tasks")

        current = await client.runs.fetch(run.benchmark_id)
        print(f"Current status: {current.details.status.value}")

        page = await client.runs.list()
        print(f"Visible runs: {page.total_count}")

        if current.details.status == BenchmarkStatus.FINISHED:
            results = await client.runs.results(run.benchmark_id)
            print(f"Retrieved results for {results.benchmark_id}")
        else:
            print("Results are available after the run reaches FINISHED")


async def main(args: argparse.Namespace) -> int:
    """Run the example and convert SDK errors into a concise CLI failure."""
    try:
        await run_lifecycle(args)
    except ValkyrieSDKError as exc:
        print(f"Valkyrie SDK error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(parse_args())))
