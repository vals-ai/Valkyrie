from typing import Any

from pydantic import BaseModel, Field


class AgentContract(BaseModel):
    """
    Declarative contract describing how to run an agent in the sandbox.

    The tracker service consumes this schema to upload agent payloads,
    run setup commands, and execute the agent CLI command.
    """

    name: str = Field(..., description="Human-readable name for the agent")
    uploads: list[str] = Field(
        default_factory=list,
        description="Relative paths to agent payloads that should be uploaded and zipped by the client",
    )
    setup: list[str] = Field(
        default_factory=list,
        description="Commands to run inside the sandbox to install or prepare the agent",
    )
    command: str = Field(..., description="CLI command to execute the agent")
    env: dict[str, Any] = Field(
        default_factory=dict,
        description="Environment variables to set when running the agent",
    )
