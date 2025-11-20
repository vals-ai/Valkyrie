"""Runner for executing agents on benchmarks."""

import argparse
import asyncio
import logging

from agentic_harness.base import Agent, Benchmark
from agentic_harness.registry import load_agent, load_benchmark, load_dataset
from model_library.base import QueryResult

from dotenv import load_dotenv

from datasets.fab.dataset import Dataset

load_dotenv()

logger = logging.getLogger(__name__)


async def basic_runner(agent: Agent, dataset: Dataset, benchmark: Benchmark):
    """
    Basic benchmark runnner.

    Eventually we will build a more robust runner that can manage tasks in a
    distributed system.

    This can also be a class.
    """
    task_groups = await dataset.create()
    for task_group in task_groups:
        for task in task_group.tasks:
            await benchmark.run(task, agent)


async def main():
    parser = argparse.ArgumentParser(description="Run an agent on a benchmark")

    parser.add_argument(
        "--agent", required=True, help="Agent name (directory in agents/)"
    )
    parser.add_argument(
        "--dataset", required=True, help="Dataset name (directory in datasets/)"
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

    logger.info(f"Loading dataset: {args.dataset}")
    dataset = load_dataset(args.dataset)

    logger.info(f"Loading benchmark: {args.benchmark}")
    benchmark = load_benchmark(args.benchmark)

    await basic_runner(agent, dataset, benchmark)


if __name__ == "__main__":
    asyncio.run(main())
