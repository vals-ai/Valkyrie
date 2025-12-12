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

    def build_execute_command(self, task: Task) -> str:
        """
        Constructs the command that we use to execute the agent inside of a sandbox. Will only append sandbox related arguments if the task has a sandbox.

        ```python
        # Example of adding additional arguments to the command
        if task.sandbox is not None:
            base_command += f" --sandbox-id {task.sandbox.id}"
        ```
        """
        base_command: str = "cd /app/agent && uv run --project hello-world python -u hello-world/hello_world.py"

        if task.sandbox is not None:
            base_command += f" --sandbox-id {task.sandbox.id}"

        return base_command

    @abstractmethod
    async def run(self, task: Task) -> QueryResult:
        """Execute the agent for the provided task and return a model response."""
