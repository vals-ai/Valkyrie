import json
import os
from typing import Any, override

from model_library.base import InputItem

from src.classes import Benchmark
from src.models import Task

from .agent import MathAgent
from .dataset import MathAgentDataset


class MathAgentBenchmark(Benchmark):
    _agent: MathAgent
    _output_dir: str

    def __init__(
        self,
        dataset: MathAgentDataset,
        agent: MathAgent,
        output_dir: str = "results/math_agent",
    ):
        super().__init__(dataset)
        self._agent = agent
        self._output_dir = output_dir

    @override
    async def prepare_task(self, task: Task) -> list[InputItem]:
        """No system prompt so we can just return the task input directly here"""

        return task.input

    @override
    async def run(self) -> None:
        dataset = await self._dataset.dataset

        for task in dataset:
            input_items = await self.prepare_task(task)

            output = await self._agent.run(input_items)

            evaluation_result = await self.evaluate(task, output)

            await self.upload_results(evaluation_result)

    @override
    async def evaluate(self, task: Task, output: dict[str, Any]) -> dict[str, Any]:
        task_answer = task.extra.get("answer", "")
        agent_answer = output.get("response", "")

        result = {
            "task_id": task.extra.get("test_id", "unknown"),
            "pass_rate": 0.0,
        }

        if agent_answer.strip() == task_answer.strip():
            result["pass_rate"] = 1.0

        return result

    @override
    async def upload_results(self, evaluation_result: dict[str, Any]) -> None:
        """
        Takes the evaluation result and saves it to a json file locally
        """
        evaluation_result_path = os.path.join(
            self._output_dir, f"{evaluation_result['task_id']}.json"
        )

        with open(evaluation_result_path, "w") as f:
            json.dump(evaluation_result, f, indent=4)
