"""Unit tests for staged egress policy ownership and composition."""

import pytest
from pydantic import ValidationError

from tracker.database.models import AgentContractRequest
from tracker.egress import EgressPolicy, combine_run_egress_policies


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


@pytest.mark.parametrize("egress", [None, {"install": "*"}, {"run": []}])
def test_agent_contract_rejects_obsolete_egress(egress: object) -> None:
    with pytest.raises(ValidationError, match="install_egress"):
        AgentContractRequest.model_validate({"name": "agent", "egress": egress})


@pytest.mark.parametrize(
    ("install_egress", "expected"),
    [
        (None, "*"),
        ("*", "*"),
        ([], []),
        (["packages.example.com"], ["packages.example.com"]),
    ],
)
def test_agent_install_egress_policy(
    install_egress: EgressPolicy | None,
    expected: EgressPolicy,
) -> None:
    contract = AgentContractRequest(name="agent", install_egress=install_egress)

    assert contract.install_egress_policy == expected
