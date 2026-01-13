from agentic_harness.base.contract import AgentContract
from agentic_harness.base.types import AgentConfig, QueryResult, Task
from agentic_harness.logger import get_logger

logger = get_logger(__name__)


class AgentRunner:
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

    _contract: AgentContract

    def __init__(self, contract: AgentContract):
        self._contract = contract

    @property
    def config(self) -> AgentConfig:
        return self._contract.config

    async def run(self, task: Task) -> QueryResult:
        """Runs the agent by calling the contract's run method"""
        response = await self._contract.run(task)

        return response
