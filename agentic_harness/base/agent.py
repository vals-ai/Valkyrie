from abc import ABC, abstractmethod

from model_library.base import InputItem, QueryResult


class Agent(ABC):
    """Interface for models that execute a single task and return a result."""

    @abstractmethod
    async def run(self, input_items: list[InputItem]) -> QueryResult:
        """Execute the agent for the provided inputs and return a model response."""
