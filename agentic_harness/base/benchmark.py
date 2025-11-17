from abc import ABC, abstractmethod

from model_library.base import InputItem
from pydantic import BaseModel

from agentic_harness.base.agent import Agent


class Task(BaseModel):
    input: list[InputItem]


class TaskGroup(BaseModel):
    tasks: list[Task]


class Benchmark(ABC):
    on_platform: bool = True

    @abstractmethod
    async def dataset(self) -> list[TaskGroup]: ...

    @abstractmethod
    async def run(self, agent: Agent) -> None: ...
