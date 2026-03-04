"""Reusable agent runner backed by model-library's Agent loop."""

import logging
from pathlib import Path

from model_library.agent.agent import Agent, AgentResult
from model_library.agent.config import AgentConfig
from model_library.agent.tool import Tool
from model_library.base.input import TextInput
from model_library.registry_utils import get_registry_model
from model_library.utils import create_file_logger


async def run_with_tools(
    tools: list[Tool],
    problem_statement: str,
    model: str,
    *,
    max_turns: int = 100,
    state: dict | None = None,
    logger: logging.Logger | None = None,
    log_file: str | Path = "agent.log",
) -> AgentResult:
    """
    Run an agent loop with the given tools and problem statement.

    Args:
        tools: List of tools available to the agent.
        problem_statement: The task description for the agent.
        model: Model registry key (e.g. "anthropic/claude-opus-4-6").
        max_turns: Maximum number of agent turns before stopping.
        state: Optional mutable state dict shared across tool calls.
        logger: Optional logger; when provided, log_file is ignored.
        log_file: Path for the log file when no logger is supplied.

    Returns:
        AgentResult with final_answer, turns, and state.
    """
    llm = get_registry_model(model)

    if logger:
        return await _run(llm, tools, logger, problem_statement, max_turns, state)

    with create_file_logger(name="agent", log_file=log_file) as file_logger:
        return await _run(llm, tools, file_logger, problem_statement, max_turns, state)


async def _run(
    llm: object,
    tools: list[Tool],
    logger: logging.Logger,
    problem_statement: str,
    max_turns: int,
    state: dict | None,
) -> AgentResult:
    agent = Agent(
        llm=llm,
        tools=tools,
        logger=logger,
        config=AgentConfig(max_turns=max_turns),
    )
    return await agent.run([TextInput(text=problem_statement)], state=state)
