from abc import ABC, abstractmethod

from model_library.base import InputItem
from pydantic import BaseModel

from agentic_harness.base.agent import Agent


class Task(BaseModel):
    """
    Represents one task.

    id field represents the run_id for platform benchmarks.
    """

    id: str
    input: list[InputItem]


class TaskGroup(BaseModel):
    """Collection of tasks."""

    tasks: list[Task]


class Benchmark(ABC):
    """Constructs dataset and runs agents against them."""

    @abstractmethod
    async def dataset(self) -> list[TaskGroup]:
        """Materialize the task groups that compose this benchmark."""

    @abstractmethod
    async def evaluate(self, task: Task, agent: Agent) -> None:
        """
        Run the agent on a single task and handle result aggregation.

        If a benchmark is coupled with the platform, this method should upload
        QuestionAnswerPairs to the platform.
        """
