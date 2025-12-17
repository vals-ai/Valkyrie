from typing_extensions import override
from vals.sdk.tracing import SpanType, TestInfo, Trace, get_client
from src.base.contract import AgentContract
from typing import Any, cast
from model_library.base import InputItem, QueryResult, QueryResultMetadata, TextInput
from model_library.registry_utils import get_registry_model
from src.base.types import AgentConfig, Task
from submodules.finance_agent.tools import (
    GoogleWebSearch,
    RetrieveInformation,
    ParseHtmlPage,
    EDGARSearch,
    Tool,
)
from submodules.finance_agent.agent import Agent as EdgarAgent
from src.utils import setup_environment

setup_environment()


class EdgarAgentContract(AgentContract):
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

    def _create_edgar_agent(self, trace: Trace | None = None) -> EdgarAgent:
        """Creates a hard coded edgar agent"""
        config = self._validate_config()

        llm = get_registry_model(cast(str, config.model))

        tools: dict[str, Tool] = {
            "google_web_search": GoogleWebSearch(),
            "retrieve_information": RetrieveInformation(),
            "parse_html_page": ParseHtmlPage(),
            "edgar_search": EDGARSearch(),
        }

        agent = EdgarAgent(llm=llm, tools=tools, max_turns=self._max_turns)

        if trace:
            agent.run = trace.span(agent.run, name="EdgarAgent", span_type=SpanType.LOGIC)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            llm.query = trace.span(llm.query, name=llm.model_name, span_type=SpanType.LLM)
            for tool in tools.values():
                tool.call_tool = trace.span(tool.call_tool, name=tool.name, span_type=SpanType.TOOL)  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]

        return agent

    def _create_query_result(self, response: str, metadata: dict[str, Any]) -> QueryResult:
        """Provided a response from the edgar agent, creates a query result object"""

        tokens_usage = metadata["total_tokens"]

        query_result_metadata = QueryResultMetadata(
            in_tokens=tokens_usage["in_tokens"],
            out_tokens=tokens_usage["out_tokens"],
            reasoning_tokens=tokens_usage["reasoning_tokens"],
            cache_read_tokens=tokens_usage["cache_read_tokens"],
            cache_write_tokens=tokens_usage["cache_write_tokens"],
            # TODO: add cost
        )

        return QueryResult(
            output_text=response,
            metadata=query_result_metadata,
            raw=metadata,
        )

    def _validate_input(self, input: list[InputItem]) -> str:
        """Validates that the task input matches the format expected by EdgarAgent."""
        if not isinstance(input[0], TextInput):
            raise ValueError(f"Edgar agent received unexpected input type: {input}")

        question = input[0].text
        return question

    @override
    async def run(self, task: Task) -> QueryResult:
        """Runs the edgar agent with a single task and parses the response into a query result object"""
        trace = None
        if self.config.trace:
            qa_set_id: str | None = task.extra.get("qa_set_id", None)
            if qa_set_id is None:
                raise ValueError("Task does not contain qa_set_id, check implementation of FinanceAgentBenchmark")

            test_info = TestInfo(test_id=task.test_id, qa_set_id=qa_set_id)

            trace = get_client(self.__class__.__name__, test_info=test_info)

        agent = self._create_edgar_agent(trace=trace)

        question = self._validate_input(task.input)

        response, metadata = await agent.run(question=question)  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]

        if trace:
            trace.shutdown()

        return self._create_query_result(response, metadata)  # pyright: ignore[reportUnknownArgumentType]
