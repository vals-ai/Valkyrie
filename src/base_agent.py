from model_library.base import QueryResult

from src.base.contract import AgentContract
from src.base.environment import Environment
from src.base.evaluate import Evaluator
from src.base.types import AgentConfig, Task
from src.logger import get_logger

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
    _evaluator: Evaluator
    _environment: Environment | None

    def __init__(self, contract: AgentContract, evaluator: Evaluator, environment: Environment | None):
        self._contract = contract
        self._evaluator = evaluator
        self._environment = environment

    @property
    def config(self) -> AgentConfig:
        return self._contract.config

    @property
    def environment(self) -> Environment | None:
        return self._environment

    @property
    def environment_variables(self) -> dict[str, str]:
        return self._contract.environment_variables

    def build_execute_command(self, task: Task) -> str:
        """Returns the build command that is executed inside of the sandbox to start the agent"""
        return self._contract.build_execute_command(task)

    async def run(self, task: Task) -> QueryResult:
        """Runs the agent by calling the contract's run method"""
        if self._environment and (not task.sandbox or not task.sandbox.id):
            raise ValueError("Environment was selected but no sandbox was created")

        response = await self._contract.run(task)

        await self._evaluator.evaluate(task, response)

        if self._environment and task.sandbox and task.sandbox.id:
            await self._environment.close(task.sandbox.id)

        return response
