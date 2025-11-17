from typing import Any, cast
from model_library.base import (
    LLM,
    InputItem,
    QueryResult,
    QueryResultMetadata,
    TextInput,
)
from typing_extensions import override
from agents.fab.edgar_agent.tool import (
    GoogleWebSearch,
    RetrieveInformation,
    ParseHtmlPage,
    EDGARSearch,
    Tool,
)
from edgar_agent.agent import Agent as EdgarAgent

from agentic_harness.base.agent import Agent


class FinanceAgent(Agent):
    def __init__(self, llm: LLM, max_turns: int = 20):
        tools: dict[str, Tool] = {
            "google_web_search": GoogleWebSearch(),
            "retrieve_information": RetrieveInformation(),
            "parse_html_page": ParseHtmlPage(),
            "edgar_search": EDGARSearch(),
        }
        self._agent = EdgarAgent(llm=llm, tools=tools, max_turns=max_turns)

    @override
    async def run(self, input_items: list[InputItem]) -> QueryResult:
        if len(input_items) != 1:
            raise ValueError("Expected exactly one input item")

        if not isinstance(input_items[0], TextInput):
            raise ValueError("Expected a TextInput")

        response, metadata = await self._agent.run(input_items[0].text)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
        metadata = cast(dict[str, Any], metadata)
        return QueryResult(
            output_text=response,
            metadata=QueryResultMetadata(
                in_tokens=metadata["total_tokens"]["prompt_tokens"],
                out_tokens=metadata["total_tokens"]["completion_tokens"],
                duration_seconds=metadata["total_duration_seconds"],
            ),
            raw=metadata,
        )
