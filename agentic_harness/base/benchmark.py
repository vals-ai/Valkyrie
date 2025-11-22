from abc import ABC, abstractmethod

from agentic_harness.base.agent import Agent
from agentic_harness.base.dataset import Dataset
from agentic_harness.base.types import TaskGroup


class Benchmark(ABC):
    _dataset: Dataset
    _agent: Agent

    def __init__(self, dataset: Dataset, agent: Agent):
        self._dataset = dataset
        self._agent = agent

    @abstractmethod
    async def setup(self) -> None:
        """Hook that runs before we instantiate the dataset and starts the agents"""
        ...

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
        await self.setup()

        task_groups = await self._dataset.create()

        for task_group in task_groups:
            await self._run_task_group(task_group)
        ```
        """

        task_groups = await self._create_dataset()

        for task_group in task_groups:
            await self._run_task_group(task_group)
