import os
from typing import Any

from typing_extensions import override

from agentic_harness.base.contract import AgentContract
from agentic_harness.base.types import QueryResult, QueryResultCost, QueryResultMetadata, Task
from contracts.claude_code.agent import ClaudeCodeAgent


class ClaudeCodeAgentContract(AgentContract):
    def _create_query_result(self, response: dict[str, Any]) -> QueryResult:
        """
        Documentation can be found here: https://code.claude.com/docs/en/headless#json-output

        Creates a query result object from the response object.
        ```json
            {
                "type": "result",
                "subtype": "success",
                "total_cost_usd": 0.003,
                "is_error": false,
                "duration_ms": 1234,
                "duration_api_ms": 800,
                "num_turns": 6,
                "result": "The response text here...",
                "session_id": "abc123"
            }
        ```
        """

        usage: dict[str, Any] = response.get("usage", {})
        metadata = QueryResultMetadata(
            cost=QueryResultCost(
                # total=response.get("total_cost_usd", 0),
                input=0,  # Required by model library
                output=0,
            ),
            duration_seconds=response.get("duration_ms", None),
            in_tokens=usage.get("input_tokens", 0),
            out_tokens=usage.get("output_tokens", 0),
            cache_read_tokens=usage.get("cache_read_input_tokens", 0),
            cache_write_tokens=usage.get("cache_creation_input_tokens", 0),
        )

        return QueryResult(
            output_text=response.get("result", None),
            metadata=metadata,
            raw=response,
        )

    @property
    @override
    def environment_variables(self) -> dict[str, str]:
        """Returns the environment variables that are required to run the agent."""

        anthropic_api_key = os.getenv("ANTHROPIC_API_KEY", None)
        if anthropic_api_key is None:
            raise ValueError("ANTHROPIC_API_KEY is not set in the environment")

        return {
            "ANTHROPIC_API_KEY": anthropic_api_key,
        }

    @override
    async def run(self, task: Task) -> QueryResult:
        agent = ClaudeCodeAgent()

        try:
            response = await agent.run(input_items=task.input)  # type: ignore

            return self._create_query_result(response)
        except Exception as e:
            return QueryResult(output_text=str(e))
