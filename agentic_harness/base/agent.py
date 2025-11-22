from abc import ABC, abstractmethod

from model_library.base import QueryResult
from agentic_harness.base.types import Task


class Agent(ABC):
    """
    Interface for models that execute a single task and return a result.

    Agent only relies on itself, containing all nessecary logic to complete a task.

    Usage example
    ```
    # Agent gets _instantiated_ with the task it needs to complete
    task = Task(id="123", input=[TextInput(text="Hello, world!")], extra={"suite_id": "123"})
    agent = Agent(task)

    # Agent can execute itself
    result = await agent.run()
    ...
    ```
    """

    @abstractmethod
    async def run(self, task: Task) -> QueryResult:
        """Execute the agent for the provided task and return a model response."""
