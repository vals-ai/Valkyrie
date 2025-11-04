"""Runner for executing agents on benchmarks."""

import logging
from pathlib import Path
from typing import Any

from agent_runner import AgentRunner
from benchmark_manager import BenchmarkRunner

logger = logging.getLogger(__name__)


def run_agent_on_benchmark(
    agent_runner: AgentRunner,
    benchmark_name: str,
    benchmarks_dir: Path = Path("benchmarks"),
) -> dict[str, Any]:
    runner = BenchmarkRunner(
        agents_dir=Path("agents"),
        config={"benchmarks_dir": str(benchmarks_dir)}
    )
    return runner.run(agent_runner, benchmark_name)


if __name__ == "__main__":
    logger.info("Implement an AgentRunner subclass and pass it to run_agent_on_benchmark()")
