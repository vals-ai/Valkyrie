from typing import Any

from pydantic import BaseModel


class AgentConfig(BaseModel):
    """Configuration for the agent."""

    model: str | None = None
    """Model key (e.g., openai/gpt-4o)"""

    kwargs: dict[str, Any] = {}
    """Additonal arguments we want to pass into the agent"""
