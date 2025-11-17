from abc import ABC, abstractmethod

from model_library.base import InputItem
from pydantic import BaseModel


class Task(BaseModel):
    input: list[InputItem]


class TaskGroup(BaseModel):
    tasks: list[Task]


class Benchmark(ABC):
    @property
    @abstractmethod
    def dataset(self) -> list[TaskGroup]: ...
