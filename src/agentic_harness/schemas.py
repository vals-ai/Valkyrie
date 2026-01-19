from pydantic import BaseModel


class AgentConfig(BaseModel):
    """Configuration for the agent."""

    model: str
    """Model key (e.g., openai/gpt-4o)"""
