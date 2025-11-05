"""Runner for executing agents on benchmarks."""

import argparse
import importlib.util
import json
import logging
import sys
from pathlib import Path
from typing import Any

from agent_runner import AgentRunner
from benchmark_runner import ValsRunner
from constants import AGENTS_DIR, DEFAULT_BENCHMARKS_DIR

logger = logging.getLogger(__name__)


def load_agent(agent_name: str) -> AgentRunner:
    """Load an agent from the agents directory.

    Args:
        agent_name: Name of the agent directory

    Returns:
        An instance of AgentRunner subclass

    Raises:
        FileNotFoundError: If agent directory or main.py not found
        ValueError: If no AgentRunner subclass found in agent module
    """
    agent_path = AGENTS_DIR / agent_name
    agent_main = agent_path / "main.py"

    if not agent_main.exists():
        raise FileNotFoundError(
            f"Agent '{agent_name}' not found at {agent_main}"
        )

    spec = importlib.util.spec_from_file_location("agent_module", agent_main)
    if spec is None or spec.loader is None:
        raise ValueError(f"Failed to load agent module from {agent_main}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Find the AgentRunner subclass
    agent_class = None
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if (isinstance(attr, type) and
            issubclass(attr, AgentRunner) and
            attr is not AgentRunner):
            agent_class = attr
            break

    if agent_class is None:
        raise ValueError(
            f"No AgentRunner subclass found in {agent_main}"
        )

    return agent_class()


def run_agent_on_benchmark(
    agent_runner: AgentRunner,
    benchmark_name: str,
    benchmarks_dir: Path = DEFAULT_BENCHMARKS_DIR,
) -> dict[str, Any]:
    runner = ValsRunner(
        agents_dir=AGENTS_DIR,
        config={"benchmarks_dir": str(benchmarks_dir)}
    )
    return runner.run(agent_runner, benchmark_name)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Run an agent on a benchmark"
    )
    parser.add_argument(
        "--agent",
        required=True,
        help="Name of the agent to run (directory name in agents/)"
    )
    parser.add_argument(
        "--benchmark",
        required=True,
        help="Name of the benchmark to run (directory name in benchmarks/)"
    )
    parser.add_argument(
        "--benchmarks-dir",
        type=Path,
        default=DEFAULT_BENCHMARKS_DIR,
        help="Path to benchmarks directory (default: benchmarks/)"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    try:
        logger.info(f"Loading agent: {args.agent}")
        agent = load_agent(args.agent)

        logger.info(f"Running agent on benchmark: {args.benchmark}")
        result = run_agent_on_benchmark(
            agent,
            args.benchmark,
            args.benchmarks_dir
        )

        print("\n" + "=" * 60)
        print("BENCHMARK RESULTS")
        print("=" * 60)
        print(f"Agent: {args.agent}")
        print(f"Benchmark: {result['benchmark']}")
        print(f"Dataset size: {result['dataset_size']}")
        print("\nOutput:")
        print(result['output'])
        if result.get('evaluation'):
            print("\nEvaluation:")
            print(json.dumps(result['evaluation'], indent=2))
        print("=" * 60 + "\n")

    except FileNotFoundError as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    except ValueError as e:
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
