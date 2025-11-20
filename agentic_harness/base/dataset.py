from abc import ABC, abstractmethod
from model_library.base import InputItem
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


class Dataset(ABC):
    """Constructs dataset for a benchmark."""

    @abstractmethod
    async def create(self) -> list[TaskGroup]:
        """Constructs dataset for a benchmark."""
