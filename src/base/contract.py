from abc import ABC, abstractmethod

from model_library.base import QueryResult

from src.base.types import AgentConfig, Task


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

    def build_execute_command(self, task: Task) -> str:
        """
        Constructs the command that we use to execute the agent inside of a sandbox. Will only append sandbox related arguments if the task has a sandbox.

        ```python
        # Example of adding additional arguments to the command
        if task.sandbox is not None:
            base_command += f" --sandbox-id {task.sandbox.id}"
        ```
        """
        base_command: str = f"/app/.venv/bin/agentic-harness --config '{self.config.model_dump_json()}' --task '{task.model_dump_json()}'"

        return base_command

    @abstractmethod
    async def run(self, task: Task) -> QueryResult:
        """Execute the agent for the provided task and return a model response."""
