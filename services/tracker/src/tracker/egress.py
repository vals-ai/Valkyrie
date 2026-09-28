"""Shared network egress policy models."""

from typing import Literal


EgressPolicy = Literal["*"] | list[str]


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
