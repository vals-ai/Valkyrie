"""Unit tests for staged egress policy ownership and composition."""

from typing import Any

import pytest
from pydantic import ValidationError

from tracker.egress import AgentEgressPlan, EgressPolicy, combine_run_egress_policies


@pytest.mark.parametrize(
    ("benchmark_policy", "legacy_agent_allowlist", "expected"),
    [
        (None, [], "*"),
        (None, ["agent.example.com"], ["agent.example.com"]),
        ("*", [], "*"),
        ("*", ["agent.example.com"], "*"),
        ([], [], []),
        ([], ["agent.example.com"], ["agent.example.com"]),
        (["benchmark.example.com"], [], ["benchmark.example.com"]),
        (
            ["shared.example.com", "benchmark.example.com"],
            ["shared.example.com", "agent.example.com"],
            ["shared.example.com", "benchmark.example.com", "agent.example.com"],
        ),
    ],
)
def test_combine_run_egress_policies(
    benchmark_policy: EgressPolicy | None,
    legacy_agent_allowlist: list[str],
    expected: EgressPolicy,
) -> None:
    assert combine_run_egress_policies(benchmark_policy, legacy_agent_allowlist) == expected


def test_agent_egress_plan_rejects_run_policy() -> None:
    payload: dict[str, Any] = {"install": "*", "run": []}

    with pytest.raises(ValidationError):
        AgentEgressPlan.model_validate(payload)
