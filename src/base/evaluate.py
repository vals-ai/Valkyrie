from abc import ABC, abstractmethod
from model_library.base import (
    QueryResult,
)
from src.base.types import Task


class Evaluator(ABC):
    """
    Evaluator that evaluates the result of the task
    """

    @abstractmethod
    async def evaluate(self, task: Task, result: QueryResult) -> None:
        """
        Using the original task construct and the result of the agent,
        evaluate the result of the task and return a score.
        """
        ...
