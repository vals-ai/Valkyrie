from abc import ABC, abstractmethod

from model_library.base import QueryResult
from agentic_harness.base.types import Task, AgentConfig


class AgentContract(ABC):
    """
    Agent contract that all submodules must implement,
    This allows us to substitute different agent scaffolds with ease.

    """

    _config: AgentConfig

    def __init__(self, config: AgentConfig):
        self._config = config

    @property
    def config(self) -> AgentConfig:
        return self._config

    @abstractmethod
    async def run(self, task: Task) -> QueryResult:
        """Execute the agent for the provided task and return a model response."""
