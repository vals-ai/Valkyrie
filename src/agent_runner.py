"""Abstract interface for running agents."""

from abc import ABC, abstractmethod
from typing import Any


class InputItem:
    """Input item for an agent."""

    def __init__(self, task_id: str, input: Any):
        self.task_id = task_id
        self.input = input


class AgentRunner(ABC):
    """Abstract interface for running agents."""

    @abstractmethod
    def run(self, input_items: list[InputItem]) -> Any:
        """Run agent on input items."""
        pass
