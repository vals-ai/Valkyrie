"""Runner for executing agents on benchmarks."""

import argparse
import asyncio

from dotenv import load_dotenv

from src.base.types import BaseConfig
from src.logger import get_logger
from src.registry import create_benchmark
from src.utils import create_base_config

load_dotenv()

logger = get_logger(__name__)


async def run_benchmark(config: BaseConfig):
    try:
        benchmark = create_benchmark(config)

        await benchmark.run()
    except ValueError as e:
        logger.error(f"Error creating benchmark: {e}")
        return


async def main():
    parser = argparse.ArgumentParser(description="Run an agent on a benchmark")

    parser.add_argument("--config", required=True, help="Path to the config file")

    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    config = create_base_config(args.config)

    logger.info(f"Running agent: `{config.agent}` on benchmark: `{config.benchmark}`")

    await run_benchmark(config)


if __name__ == "__main__":
    asyncio.run(main())
