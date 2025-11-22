from typing import Any
from model_library.base import (
    QueryResult,
    QueryResultMetadata,
    TextInput,
)
from model_library.registry_utils import get_registry_model
from typing_extensions import override
from agentic_harness.base.types import Task
from agents.fab.edgar_agent.tool import (
    GoogleWebSearch,
    RetrieveInformation,
    ParseHtmlPage,
    EDGARSearch,
    Tool,
)
from agentic_harness.base.agent import Agent
from .edgar_agent.agent import Agent as EdgarAgent


class FinanceAgent(Agent):
    _MAX_TURNS: int = 20

    def _create_edgar_agent(self) -> EdgarAgent:
        """Creates a hard coded edgar agent"""
        llm = get_registry_model("openai/gpt-5-mini-2025-08-07")
        tools: dict[str, Tool] = {
            "google_web_search": GoogleWebSearch(),
            "retrieve_information": RetrieveInformation(),
            "parse_html_page": ParseHtmlPage(),
            "edgar_search": EDGARSearch(),
        }

        return EdgarAgent(llm=llm, tools=tools, max_turns=self._MAX_TURNS)

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
            ),
            raw=metadata,
        )

    @override
    async def run(self, task: Task) -> QueryResult:
        """Runs the edgar agent with a single task and parses the response into a query result object"""
        agent = self._create_edgar_agent()

        input_item = task.input[0]
        if not isinstance(input_item, TextInput):
            raise ValueError("Expected a TextInput")

        response, metadata = await agent.run(question=input_item.text)

        return self._create_query_result(response, metadata)
