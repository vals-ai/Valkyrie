"""Benchmark manager for loading and running benchmarks."""

import importlib.util
import logging
from pathlib import Path
from types import ModuleType
from typing import Any

logger = logging.getLogger(__name__)


class BenchmarkManager:
    """Manager for loading and running benchmarks."""

    def __init__(self, agents_dir: Path):
        """
        Args:
            config: Optional configuration dictionary
        """
        self.agents_dir = agents_dir
        self.benchmarks_dir = Path(self.config.get("benchmarks_dir", "benchmarks"))

    def get_benchmark(self, benchmark_name: str):
        benchmark_path = self.benchmarks_dir / benchmark_name

        if not benchmark_path.exists():
            raise FileNotFoundError(f"Benchmark not found: {benchmark_name} at {benchmark_path}")

        # load benchmark as a module or object
        benchmark_module = self._load_benchmark_module(benchmark_path)

        # create a simple benchmark object with agent_args
        class Benchmark:
            def __init__(self, module: ModuleType, name: str):
                self.module = module
                self.name = name
                self.agent_args: dict[str, Any] = {}
                self.requires_sandbox = getattr(module, "requires_sandbox", False)
                self.setup_script = getattr(module, "setup_script", None)

            def get_dataset(self) -> dict[str, Any]:
                """Get the benchmark dataset."""
                if not hasattr(self.module, "get_dataset"):
                    raise NotImplementedError(f"Benchmark {self.name} must implement get_dataset()")

                # TODO(Nikil): assumes the benchmark module's main.py has a get_dataset function
                # need to think more about this design- perhaps it actually works well though....
                return self.module.get_dataset()

            def evaluate_output(self, agent_output: dict[str, Any], run_id: str) -> dict[str, Any]:
                """Evaluate agent solutions."""
                if not hasattr(self.module, "evaluate_output"):
                    raise NotImplementedError(
                        f"Benchmark {self.name} must implement evaluate_output()"
                    )

                # TODO(Nikil): how can we do this via our checks/operators infra?
                return self.module.evaluate_output(agent_output, run_id)

        benchmark = Benchmark(benchmark_module, benchmark_name)
        return benchmark

    def _load_benchmark_module(self, benchmark_path: Path) -> ModuleType:
        """Load benchmark module from path."""

        benchmark_main = benchmark_path / "main.py"
        if not benchmark_main.exists():
            raise FileNotFoundError(f"Benchmark main.py not found at {benchmark_main}")

        spec = importlib.util.spec_from_file_location("benchmark", benchmark_main)
        if spec is None or spec.loader is None:
            raise ValueError(f"Failed to load benchmark module from {benchmark_main}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
