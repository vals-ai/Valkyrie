from pydantic import BaseModel


class AgentConfig(BaseModel):
    """Configuration for the agent."""

    model: str | None = None
    """Model key (e.g., openai/gpt-4o)"""
