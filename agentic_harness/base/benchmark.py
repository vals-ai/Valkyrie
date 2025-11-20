from abc import ABC, abstractmethod

from agentic_harness.base.dataset import Task

from model_library.base import QueryResult


class Benchmark(ABC):
    """Constructs dataset and runs agents against them."""

    @abstractmethod
    async def evaluate(self, task: Task, query_result: QueryResult) -> None:
        """Evaluate query result for a single task."""
