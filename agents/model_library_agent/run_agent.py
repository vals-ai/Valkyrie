"""Reusable agent runner backed by model-library's Agent loop."""

import logging

from model_library.agent.agent import Agent, AgentResult
from model_library.agent.config import AgentConfig
from model_library.agent.tool import Tool
from model_library.base.input import TextInput
from model_library.registry_utils import get_registry_model


async def run_with_tools(
    tools: list[Tool],
    problem_statement: str,
    model: str,
    *,
    max_turns: int = 100,
    state: dict | None = None,
    logger: logging.Logger | None = None,
) -> AgentResult:
    """
    Run an agent loop with the given tools and problem statement.

    Args:
        tools: List of tools available to the agent.
        problem_statement: The task description for the agent.
        model: Model registry key (e.g. "anthropic/claude-opus-4-6").
        max_turns: Maximum number of agent turns before stopping.
        state: Optional mutable state dict shared across tool calls.
        logger: Optional logger; defaults to the module logger.

    Returns:
        AgentResult with final_answer, turns, and state.
    """
    llm = get_registry_model(model)
    agent = Agent(
        llm=llm,
        tools=tools,
        logger=logger or logging.getLogger(__name__),
        config=AgentConfig(max_turns=max_turns),
    )
    return await agent.run([TextInput(text=problem_statement)], state=state)
