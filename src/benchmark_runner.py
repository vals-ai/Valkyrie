"""Benchmark runner orchestration and implementations."""

import importlib.util
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from types import ModuleType
from typing import Any, Optional

from vals_model_proxy.base import InputItem

from agent_runner import AgentRunner

logger = logging.getLogger(__name__)


class BenchmarkRunner(ABC):
    """Abstract benchmark runner orchestrator."""

    def __init__(self, agent_runner: AgentRunner, input_data: Any):
        self._agent_runner = agent_runner
        self._input_data = input_data

    @abstractmethod
    def get_input_items(self) -> list[InputItem]:
        pass

    @abstractmethod
    def run(self, input_items: list[InputItem]) -> Any:
        pass

    @abstractmethod
    def eval(self, final_output: Any) -> Optional[dict[str, Any]]:
        pass

    def execute(self) -> dict[str, Any]:
        input_items = self.get_input_items()
        logger.info(f"Running agent on {len(input_items)} tasks")
        output = self.run(input_items)
        evaluation = self.eval(output)
        return {"output": output, "evaluation": evaluation}


class DefaultBenchmarkRunner(BenchmarkRunner):
    """Default benchmark runner implementation."""

    def __init__(self, agent_runner: AgentRunner, benchmark: Any, dataset: dict[str, Any]):
        super().__init__(agent_runner, dataset)
        self._benchmark = benchmark

    def get_input_items(self) -> list[InputItem]:
        return [
            {"task_id": task_id, "input": task_data}
            for task_id, task_data in self._input_data.items()
        ]

    def run(self, input_items: list[InputItem]) -> Any:
        return self._agent_runner.run(input_items)

    def eval(self, final_output: Any) -> Optional[dict[str, Any]]:
        if not hasattr(self._benchmark, "evaluate_output"):
            return None
        return self._benchmark.evaluate_output(final_output, run_id="test_run")


class ValsRunner:
    """Loads and runs benchmarks."""

    def __init__(self, agents_dir: Path, config: Optional[dict[str, Any]] = None):
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

            def evaluate_output(
                self, output: dict[str, Any], run_id: str = "test_run"
            ) -> Optional[dict[str, Any]]:
                if not hasattr(self.module, "evaluate_output"):
                    return None
                return self.module.evaluate_output(output, run_id)

        return Benchmark(benchmark_module, benchmark_name)

    def run(self, agent_runner: AgentRunner, benchmark_name: str) -> dict[str, Any]:
        benchmark = self.load_benchmark(benchmark_name)
        dataset = benchmark.get_dataset()
        runner = DefaultBenchmarkRunner(agent_runner, benchmark, dataset)
        result = runner.execute()
        result["benchmark"] = benchmark_name
        result["dataset_size"] = len(dataset)
        return result

    def _load_module(self, benchmark_path: Path) -> ModuleType:
        benchmark_main = benchmark_path / "main.py"
        if not benchmark_main.exists():
            raise FileNotFoundError(f"Benchmark main.py not found at {benchmark_main}")

        spec = importlib.util.spec_from_file_location("benchmark", benchmark_main)
        if spec is None or spec.loader is None:
            raise ValueError(f"Failed to load benchmark module from {benchmark_main}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
