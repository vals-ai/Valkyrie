"""Agent runner for executing agents on benchmarks."""

import logging
from pathlib import Path
from typing import Any

from src.benchmark_manager import BenchmarkManager

logger = logging.getLogger(__name__)

AGENTS_DIR = Path("agents")


class AgentRunner:
    """Runs an agent on a benchmark."""

    def __init__(
        self,
        agent_function: str,
        agent_dir: Path,
        agent_args: dict[str, Any],
        benchmark_name: str,
        config: dict[str, Any] | None = None,
        use_vm: bool = False,
    ):
        """Initialize the runner.

        Args:
            agent_function: Function to call in format 'module.function' (e.g., 'main.run')
            agent_dir: Path to the agent directory
            agent_args: Arguments to pass to the agent
            benchmark_name: Name of the benchmark to run
            use_vm: Whether to run on a VM
        """
        # validate agent_function format
        if "." not in agent_function:
            raise ValueError("Invalid agent_function format")

        module_name, func_name = agent_function.rsplit(".", 1)
        if not module_name or not func_name:
            raise ValueError("Invalid agent_function format")

        agents_dir = AGENTS_DIR

        # initialize benchmark manager first
        self.benchmark_manager = BenchmarkManager(agents_dir, config)
        self.benchmark = self.benchmark_manager.get_benchmark(benchmark_name)
        self.benchmark.agent_args = agent_args

        self.agent_function = agent_function
        self.agent_dir = agent_dir
        self.agent_args = agent_args
        self.use_vm = use_vm

    def run(self, **kwargs: Any) -> dict[str, Any]:
        """Run the agent on the benchmark."""
        logger.info("Running agent %s on benchmark %s", self.agent_function, self.benchmark.name)
        logger.info("Agent directory: %s", self.agent_dir)
        logger.info("Use VM: %s", self.use_vm)

        # TODO(Nikil): implement actual execution
        # this could be as simple as calling an agent function via the module
        # So that way, all the benchmark logic is encapsulated in the benchmark module

        return {}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run an agent on a benchmark")
    parser.add_argument("--agent-function", required=True, help="Agent function (e.g., 'main.run')")
    parser.add_argument("--agent-dir", type=Path, required=True, help="Path to agent directory")
    parser.add_argument("--benchmark", required=True, help="Benchmark name")
    parser.add_argument("--use-vm", action="store_true", help="Run on VM")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    runner = AgentRunner(
        agent_function=args.agent_function,
        agent_dir=args.agent_dir,
        agent_args={},
        benchmark_name=args.benchmark,
        use_vm=args.use_vm,
    )
    runner.run()
