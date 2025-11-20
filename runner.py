"""Runner for executing agents on benchmarks."""

import argparse
import asyncio
import logging

from agentic_harness.base import Agent, Benchmark
from agentic_harness.registry import load_agent, load_benchmark
from model_library.base import QueryResult

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


async def basic_runner(agent: Agent, benchmark: Benchmark) -> list[QueryResult]:
    """
    Basic benchmark runnner.

    Eventually we will build a more robust runner that can manage tasks in a
    distributed system.

    This can also be a class.
    """
    results: list[QueryResult] = []
    dataset = await benchmark.dataset()
    for task_group in dataset:
        for task in task_group.tasks:
            result = await agent.run(task.input)
            await benchmark.evaluate(task, result)
    return results


async def main():
    parser = argparse.ArgumentParser(description="Run an agent on a benchmark")

    parser.add_argument(
        "--agent", required=True, help="Agent name (directory in agents/)"
    )
    parser.add_argument(
        "--benchmark", required=True, help="Benchmark name (directory in benchmarks/)"
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger.info(f"Loading agent: {args.agent}")
    agent = load_agent(args.agent)

    logger.info(f"Loading benchmark: {args.benchmark}")
    benchmark = load_benchmark(args.benchmark)

    results = await basic_runner(agent, benchmark)
    return results


if __name__ == "__main__":
    result = asyncio.run(main())
    print(result)
