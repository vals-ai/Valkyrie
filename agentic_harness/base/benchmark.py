from abc import ABC, abstractmethod

from model_library.base import InputItem, QueryResult
from pydantic import BaseModel


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
    async def evaluate(self, task: Task, query_result: QueryResult) -> None:
        """Evaluate query result for a single task."""
