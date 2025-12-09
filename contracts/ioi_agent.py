from agentic_harness.base.contract import AgentContract
from typing import Any, cast, override
from model_library.base import QueryResult, QueryResultMetadata
from model_library.registry_utils import get_registry_model
from agentic_harness.base.types import AgentConfig, Task
from submodules.ioi_agent.tool import (
    Tool,
    Submission,
    CppExecutor,
)
from submodules.ioi_agent.agent import Agent as IOIAgent
from agentic_harness.utils import setup_environment


setup_environment()


class IOIAgentContract(AgentContract):
    """
    Contract for the Edgar Agent to be usable within the agentic harness framework
    """

    _max_turns: int = 20

    def _validate_config(self) -> AgentConfig:
        """Validate required parameters"""
        if self._config.model is None:
            raise ValueError("`model` is required")

        if max_turns := self._config.extra.get("max_turns", None):
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

    def _create_query_result(
        self, response: str, metadata: dict[str, Any]
    ) -> QueryResult:
        """Provided a response from the edgar agent, creates a query result object"""
        return QueryResult(
            output_text=response,
            metadata=QueryResultMetadata(
                in_tokens=metadata["total_tokens"]["prompt_tokens"],
                out_tokens=metadata["total_tokens"]["completion_tokens"],
                duration_seconds=metadata["total_duration_seconds"],
                cost=metadata["total_cost"],
            ),
            raw=metadata,
        )

    @override
    async def run(self, task: Task) -> QueryResult:
        """Runs the ioi agent with a single task and parses the response into a query result object"""
        agent = self._create_ioi_agent()

        response, metadata = await agent.run(input_items=task.input)

        return self._create_query_result(response, metadata)
