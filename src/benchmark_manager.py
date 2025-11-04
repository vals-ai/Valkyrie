"""Unified benchmark manager for loading and running benchmarks."""

import importlib.util
import logging
from pathlib import Path
from types import ModuleType
from typing import Any

logger = logging.getLogger(__name__)


class BenchmarkRunner:
    """Unified manager for loading and running agents on benchmarks."""

    def __init__(self, agents_dir: Path, config: dict[str, Any] | None = None):
        self.agents_dir = agents_dir
        self.config = config or {}
        self.benchmarks_dir = Path(self.config.get("benchmarks_dir", "benchmarks"))

    def load_benchmark(self, benchmark_name: str):
        benchmark_path = self.benchmarks_dir / benchmark_name

        if not benchmark_path.exists():
            raise FileNotFoundError(f"Benchmark not found: {benchmark_name} at {benchmark_path}")

        benchmark_module = self._load_module(benchmark_path)

        class Benchmark:
            def __init__(self, module: ModuleType, name: str):
                self.module = module
                self.name = name
                self.requires_sandbox = getattr(module, "requires_sandbox", False)
                self.setup_script = getattr(module, "setup_script", None)

            def get_dataset(self) -> dict[str, Any]:
                if not hasattr(self.module, "get_dataset"):
                    raise NotImplementedError(f"Benchmark {self.name} must implement get_dataset()")
                return self.module.get_dataset()

            def evaluate(
                self, output: dict[str, Any], run_id: str = "test_run"
            ) -> dict[str, Any] | None:
                if not hasattr(self.module, "evaluate_output"):
                    return None
                return self.module.evaluate_output(output, run_id)

        return Benchmark(benchmark_module, benchmark_name)

    def run(self, agent_runner: Any, benchmark_name: str) -> dict[str, Any]:
        benchmark = self.load_benchmark(benchmark_name)
        dataset = benchmark.get_dataset()
        input_items = self._prepare_input(dataset)

        logger.info(f"Running agent on {len(input_items)} tasks")
        output = agent_runner.run(input_items)
        evaluation = benchmark.evaluate(output)

        return {
            "benchmark": benchmark_name,
            "output": output,
            "evaluation": evaluation,
            "dataset_size": len(dataset),
        }

    def _prepare_input(self, dataset: dict[str, Any]) -> list[Any]:
        try:
            from agent_base import InputItem
        except ImportError:
            class InputItem:  # type: ignore
                def __init__(self, task_id: str, input: Any):
                    self.task_id = task_id
                    self.input = input

        return [
            InputItem(task_id=task_id, input=task_data)
            for task_id, task_data in dataset.items()
        ]

    def _load_module(self, benchmark_path: Path) -> ModuleType:
        benchmark_main = benchmark_path / "main.py"
        if not benchmark_main.exists():
            msg = f"Benchmark main.py not found at {benchmark_main}"
            raise FileNotFoundError(msg)

        spec = importlib.util.spec_from_file_location("benchmark", benchmark_main)
        if spec is None or spec.loader is None:
            msg = f"Failed to load benchmark module from {benchmark_main}"
            raise ValueError(msg)

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
