"""Runner for executing agents on benchmarks."""

import argparse
import importlib.util
import json
import logging
import sys
from pathlib import Path
from typing import Any

from src.agent_runner import AgentRunner
from src.benchmark_runner import ValsRunner
from src.constants import AGENTS_DIR, DEFAULT_BENCHMARKS_DIR

logger = logging.getLogger(__name__)


def load_agent(agent_name: str) -> AgentRunner:
    """Load an AgentRunner subclass from agents/{agent_name}/main.py."""
    agent_main = AGENTS_DIR / agent_name / "main.py"
    if not agent_main.exists():
        raise FileNotFoundError(f"Agent not found at {agent_main}")

    spec = importlib.util.spec_from_file_location("agent_module", agent_main)
    if spec is None or spec.loader is None:
        raise ValueError(f"Failed to load agent module from {agent_main}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if isinstance(attr, type) and issubclass(attr, AgentRunner) and attr is not AgentRunner:
            return attr()

    raise ValueError(f"No AgentRunner subclass found in {agent_main}")


def run_agent_on_benchmark(
    agent_runner: AgentRunner,
    benchmark_name: str,
    benchmarks_dir: Path = DEFAULT_BENCHMARKS_DIR,
) -> dict[str, Any]:
    runner = ValsRunner(agents_dir=AGENTS_DIR, config={"benchmarks_dir": str(benchmarks_dir)})
    return runner.run(agent_runner, benchmark_name)


def main():
    parser = argparse.ArgumentParser(description="Run an agent on a benchmark")
    parser.add_argument("--agent", required=True, help="Agent name (directory in agents/)")
    parser.add_argument(
        "--benchmark", required=True, help="Benchmark name (directory in benchmarks/)"
    )
    parser.add_argument(
        "--benchmarks-dir", type=Path, default=DEFAULT_BENCHMARKS_DIR, help="Benchmarks directory"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        logger.info(f"Loading agent: {args.agent}")
        agent = load_agent(args.agent)

        logger.info(f"Running agent on benchmark: {args.benchmark}")
        result = run_agent_on_benchmark(agent, args.benchmark, args.benchmarks_dir)

        print("\n" + "=" * 60)
        print("BENCHMARK RESULTS")
        print("=" * 60)
        print(f"Agent: {args.agent}")
        print(f"Benchmark: {result['benchmark']}")
        print(f"Dataset size: {result['dataset_size']}")
        print("\nOutput:")
        print(result["output"])
        if result.get("evaluation"):
            print("\nEvaluation:")
            print(json.dumps(result["evaluation"], indent=2))
        print("=" * 60 + "\n")

    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
