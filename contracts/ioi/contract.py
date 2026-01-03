from typing import Any, cast

from model_library.base import QueryResult, QueryResultMetadata
from model_library.registry_utils import get_registry_model
from typing_extensions import override

from agentic_harness.base.contract import AgentContract
from agentic_harness.base.types import AgentConfig, Task
from agentic_harness.utils import setup_vals_environment
from submodules.ioi_agent.agent import Agent as IOIAgent
from submodules.ioi_agent.tool import (
    CppExecutor,
    Submission,
    Tool,
)

setup_vals_environment()


class IOIAgentContract(AgentContract):
    """
    Contract for the IOI Agent to be usable within the agentic harness framework
    """

    _max_turns: int = 20

    def _validate_config(self) -> AgentConfig:
        """Validate required parameters"""
        if self._config.model is None:
            raise ValueError("`model` is required")

        if max_turns := self._config.kwargs.get("max_turns", None):
            self._max_turns = max_turns

        return self._config

    def _create_ioi_agent(self) -> IOIAgent:
        """Creates a hard coded ioi agent"""
        config = self._validate_config()
        llm = get_registry_model(cast(str, config.model))

        tools: dict[str, Tool] = {
            "cpp_executor": CppExecutor(),
            "submission": Submission(),
        }

        return IOIAgent(llm=llm, tools=tools, max_turns=self._max_turns)

    def _create_query_result(self, response: str, metadata: dict[str, Any]) -> QueryResult:
        """Provided a response from the edgar agent, creates a query result object"""
        return QueryResult(
            output_text=response,
            metadata=QueryResultMetadata(**metadata["total_metadata"]),
            raw=metadata,
        )

    @override
    async def run(self, task: Task) -> QueryResult:
        """Runs the ioi agent with a single task and parses the response into a query result object"""
        agent = self._create_ioi_agent()

        response, metadata = await agent.run(input_items=task.input)

        return self._create_query_result(response, metadata)
