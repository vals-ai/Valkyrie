import asyncio
from asyncio import Semaphore

from src.base.dataset import Dataset
from src.base.environment import Environment
from src.base.types import Task, TaskGroup
from src.base_agent import AgentRunner
from src.logger import get_logger

logger = get_logger(__name__)


class BenchmarkRunner:
    _dataset: Dataset
    _agent: AgentRunner
    _environment: Environment | None
    _semaphore: Semaphore

    def __init__(self, dataset: Dataset, agent: AgentRunner, environment: Environment | None):
        self._dataset = dataset
        self._agent = agent
        self._environment = environment
        self._semaphore = Semaphore(self._agent.config.parallelism)

    @property
    def agent(self) -> AgentRunner:
        return self._agent

    async def _run_task(self, task: Task) -> None:
        """Executes the task either inside of the environment or directly inside of the users machine"""

        if self._environment:
            await self._environment.create(task, self._agent)
        else:
            await self._agent.run(task)

    async def _run_task_group(self, task_group: TaskGroup) -> None:
        """Executes a single task group, setting up the environment before it executes the tasks"""

        # Setup the environment if it exists
        if self._environment:
            logger.info(f"Environment has been detected, setting up {self._environment.__class__.__name__}")
            await self._environment.setup()
        else:
            logger.info("No environment has been detected, running directly inside of the users machine")

        # Prepare concurrency
        async def _run_task_with_semaphore(task: Task) -> None:
            async with self._semaphore:
                await self._run_task(task)

        # Execute tasks in parallel
        await asyncio.gather(*[_run_task_with_semaphore(task) for task in task_group.tasks])

    async def _create_dataset(self) -> list[TaskGroup]:
        """Initializes the dataset for the benchmark"""
        return await self._dataset.create()

    async def run(self) -> None:
        """
        Runs complete benchmark
        1. Instantiates the dataset
        2. Runs each task group

        ```python
        task_groups = await self._dataset.create()

        for task_group in task_groups:
            await self._run_task_group(task_group)
        ```
        """

        task_groups = await self._create_dataset()

        logger.info(f"Created {len(task_groups)} task groups")

        for index, task_group in enumerate(task_groups):
            logger.info(f"Running task group {index + 1} of {len(task_groups)}")
            await self._run_task_group(task_group)
