"""Abstract interface for running agents."""

from abc import ABC, abstractmethod
from typing import Any

from vals_model_proxy.base import InputItem


class AgentRunner(ABC):
    """Abstract interface for running agents."""

    @abstractmethod
    def run(self, input_items: list[InputItem]) -> Any:
        """Run agent on input items."""
        pass
