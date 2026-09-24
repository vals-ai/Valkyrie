"""Shared network egress policy models."""

from typing import Literal

from pydantic import BaseModel, ConfigDict


EgressPolicy = Literal["*"] | list[str]


class AgentEgressPlan(BaseModel):
    """Agent-owned network policy for dependency installation."""

    model_config = ConfigDict(extra="forbid")

    install: EgressPolicy = "*"


def combine_run_egress_policies(
    benchmark_policy: EgressPolicy | None,
    legacy_agent_allowlist: list[str],
) -> EgressPolicy:
    """Combine benchmark run policy with legacy agent-requested destinations."""
    if benchmark_policy is None:
        return legacy_agent_allowlist or "*"
    if benchmark_policy == "*":
        return "*"
    return list(dict.fromkeys([*benchmark_policy, *legacy_agent_allowlist]))
