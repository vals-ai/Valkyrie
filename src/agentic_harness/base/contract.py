from abc import ABC, abstractmethod

from agentic_harness.base.types import AgentConfig, QueryResult, Task


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

    @property
    def environment_variables(self) -> dict[str, str]:
        """
        Override this method to inject additional environment variables into the sandbox that allow the agent to function


        Example:
        ```python
        import os

        return {
            "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY"),
        }
        ```
        """
        return {}

    @abstractmethod
    async def run(self, task: Task) -> QueryResult:
        """Execute the agent for the provided task and return a model response."""
