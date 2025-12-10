from abc import ABC

from src.base.dataset import Dataset
from src.base.types import TaskGroup
from src.base_agent import BaseAgent


class Benchmark(ABC):
    _dataset: Dataset
    _agent: BaseAgent

    def __init__(self, dataset: Dataset, agent: BaseAgent):
        self._dataset = dataset
        self._agent = agent

    @property
    def agent(self) -> BaseAgent:
        return self._agent

    async def _run_task_group(self, task_group: TaskGroup) -> None:
        for task in task_group.tasks:
            await self._agent.run(task)

    async def _create_dataset(self) -> list[TaskGroup]:
        return await self._dataset.create()

    async def run(self) -> None:
        """
        Runs complete benchmark
        1. Instantiates the dataset
        2. Runs each task group

        Example:
        ```python
        task_groups = await self._dataset.create()

        for task_group in task_groups:
            await self._run_task_group(task_group)
        ```
        """

        task_groups = await self._create_dataset()

        for task_group in task_groups:
            await self._run_task_group(task_group)
