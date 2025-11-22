This file contains instructions on contract use and explanation

---

The agent contract allows us to hotswap agent scaffolds between benchmarks.
We define a generic run method which calls the agentic contracts run method.

This is an example of what that looks like

```python
from abc import ABC, abstractmethod

from model_library.base import QueryResult
from agentic_harness.base.types import AgentConfig, Task
from agentic_harness.base.contract import AgentContract


class Agent(ABC):
    @abstractmethod
    async def evaluate_result(self, task: Task, result: QueryResult) -> None:
        """Evaluates the result of the task, different for each agent"""
        ...

    async def run(self, task: Task) -> QueryResult:
        """Runs the agent by calling the contract's run method"""
        response = await self._contract.run(task)

        await self.evaluate_result(task, response)

        return response
```

We define the evaluate part so that you do not have to, this allows us to mantain run method isolation, while
keeping the harness flexible and generic. This also means that all dependencies are mantained inside of the `Agent` class. If we needed to change the environment where the data from `Benchmark` is not available- the agent can survive on its own.

Below is the contract that you will be filling out

```python
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
```

How do I select my contract inside of the config?

Inside of `config/your_config.yaml` you must define the agent

```yaml
agent_config:
  model: openai/gpt-5-mini-2025-08-07
  extra:
    max_turns: 20
    verbosity: medium

benchmark: fab
agent: edgar_agent
```

When you define `agent: edgar_agent` we know to search `contracts.edgar_agent.contract`

NOTE: All settings defined under `agent_config` will be exposed from `AgentConfig`
