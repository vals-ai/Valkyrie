import asyncio
from abc import ABC
from asyncio import Semaphore

from src.base.dataset import Dataset
from src.base.environment import Environment
from src.base.types import Task, TaskGroup
from src.base_agent import BaseAgent


class Benchmark(ABC):
    _dataset: Dataset
    _agent: BaseAgent
    _environment: Environment | None
    _semaphore: Semaphore

    def __init__(self, dataset: Dataset, agent: BaseAgent, environment: Environment | None):
        self._dataset = dataset
        self._agent = agent
        self._environment = environment
        self._semaphore = Semaphore(self._agent.config.parallelism)

    @property
    def agent(self) -> BaseAgent:
        return self._agent

    async def _run_task_group(self, task_group: TaskGroup) -> None:
        # Setup the environment if it exists
        if self._environment is not None:
            await self._environment.setup()

        # Prepare concurrency
        async def _run_task_with_semaphore(task: Task) -> None:
            async with self._semaphore:
                await self._agent.run(task)

        # Execute tasks in parallel
        await asyncio.gather(*[_run_task_with_semaphore(task) for task in task_group.tasks])

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
