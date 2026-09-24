"""Shared network egress policy models."""

from typing import Literal

from pydantic import BaseModel


EgressPolicy = Literal["*"] | list[str]


class AgentEgressPlan(BaseModel):
    """Agent-owned network policy for dependency installation and execution."""

    install: EgressPolicy = "*"
    run: EgressPolicy = "*"
