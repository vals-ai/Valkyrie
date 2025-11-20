from abc import ABC, abstractmethod

from agentic_harness.base.agent import Agent
from agentic_harness.base.dataset import Task


class Benchmark(ABC):
    """Constructs dataset and runs agents against them."""

    @abstractmethod
    async def run(self, task: Task, agent: Agent) -> None:
        """Run agent against a single task."""
