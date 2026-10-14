"""Behavioral tests for SDK-owned request and response models.

Run: uv run pytest tests/unit/sdk/test_models.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from valkyrie.sdk import ValkyrieContractError
from valkyrie.sdk.models import (
    AgentContract,
    AgentContractRequest,
    FetchBenchmarkResponse,
    FetchBenchmarksResponse,
    FinalViewResponse,
    OutputArtifact,
    StartBenchmarkRequest,
)

FIXTURES = Path(__file__).parents[2] / "fixtures" / "sdk_api"


def load_fixture(name: str) -> dict[str, object]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


_CONTRACT = """\
name: demo
install_cmd: bash setup.sh
run_cmd: demo --task {problem_statement_path} --model {model} --temperature {temperature}
defaults:
  model: {type: str, required: true, choices: [openai/gpt-5, anthropic/claude-opus-5]}
kwargs:
  temperature: {type: float, required: false, default: 0.7}
"""


class TestAgentContract:
    """Reading contract.yaml and checking run kwargs before anything is uploaded."""

    def test_validate_kwargs_coerces_cli_strings_and_fills_defaults(self, tmp_path: Path) -> None:
        (tmp_path / "contract.yml").write_text(_CONTRACT)
        contract = AgentContract.from_directory(tmp_path)

        assert contract.validate_kwargs({}, model="openai/gpt-5") == {"model": "openai/gpt-5", "temperature": 0.7}
        assert contract.validate_kwargs({"temperature": "1.5", "model": "anthropic/claude-opus-5"}) == {
            "model": "anthropic/claude-opus-5",
            "temperature": 1.5,
        }

        with pytest.raises(ValkyrieContractError) as error:
            contract.validate_kwargs({"temperature": "hot"})

        assert str(error.value).splitlines() == [
            "  - 'model' is required but was not provided",
            "  - 'temperature': Input should be a valid number, unable to parse string as a number",
        ]

    @pytest.mark.parametrize(
        ("files", "message"),
        [
            ({}, "No contract.yaml or contract.yml in"),
            ({"contract.yaml": "name: [unclosed"}, "Failed to parse YAML contract from `agent/contract.yaml`"),
            ({"contract.yaml": "- not a mapping"}, "Invalid contract `agent/contract.yaml`: expected a mapping"),
            (
                {"contract.yaml": "name: demo\ninstall_cmd: x\nrun_cmd: demo"},
                "Invalid contract `agent/contract.yaml`\n  - 'run_cmd': Value error, run_cmd must contain",
            ),
            (
                {"contract.yaml": _CONTRACT + "output_artifacts: [/tmp/result]\n"},
                "'output_artifacts': Value error, output_artifacts paths must be relative paths",
            ),
        ],
        ids=["missing", "bad-yaml", "not-a-mapping", "invalid-field", "absolute-artifact"],
    )
    def test_from_directory_reports_unusable_contracts(
        self, tmp_path: Path, files: dict[str, str], message: str
    ) -> None:
        agent_path = tmp_path / "agent"
        agent_path.mkdir()
        for name, content in files.items():
            (agent_path / name).write_text(content)

        with pytest.raises(ValkyrieContractError, match=re.escape(message)):
            AgentContract.from_directory(agent_path)


def test_agent_contract_serializes_install_egress() -> None:
    legacy_contract = AgentContractRequest(name="legacy")
    staged_contract = AgentContractRequest(
        name="staged",
        install_egress=["https://packages.example.com"],
    )

    assert legacy_contract.model_dump(mode="json")["install_egress"] is None
    assert staged_contract.model_dump(mode="json")["install_egress"] == ["https://packages.example.com"]


@pytest.mark.parametrize(
    "policy",
    ["packages.example.com", ["packages.example.com", 1], {"install": "*"}],
)
def test_agent_contract_rejects_invalid_install_egress(policy: object) -> None:
    with pytest.raises(ValidationError):
        AgentContractRequest.model_validate({"name": "agent", "install_egress": policy})


def test_agent_contract_allows_install_and_legacy_run_egress_together() -> None:
    contract = AgentContractRequest(
        name="combined",
        egress_allowlist=["legacy.example.com"],
        install_egress=["packages.example.com"],
    )

    assert contract.egress_allowlist == ["legacy.example.com"]
    assert contract.install_egress == ["packages.example.com"]


@pytest.mark.parametrize("egress", [None, {"install": "*"}, {"run": []}])
def test_agent_contract_rejects_obsolete_egress(egress: object) -> None:
    with pytest.raises(ValidationError, match="install_egress"):
        AgentContractRequest.model_validate({"name": "agent", "egress": egress})


def test_agent_contract_normalizes_output_artifacts() -> None:
    contract = AgentContractRequest(
        name="agent",
        output_artifacts=[
            "reports/result.json",
            {"path": "logs/run.txt", "source": "/workspace/logs/*.txt"},
            {
                "path": "artifacts/model.patch",
                "source": "/logs/artifacts/model.patch",
                "required": False,
            },
        ],
    )

    assert contract.output_artifacts[0] == "reports/result.json"
    assert contract.output_artifacts[1] == OutputArtifact(
        path="logs/run.txt",
        source="/workspace/logs/*.txt",
    )
    assert contract.output_artifacts[2] == OutputArtifact(
        path="artifacts/model.patch",
        source="/logs/artifacts/model.patch",
        required=False,
    )

    serialized = contract.model_dump(mode="json")["output_artifacts"]
    assert "required" not in serialized[1]
    assert serialized[2]["required"] is False


@pytest.mark.parametrize("path", ["/absolute", "../escape", "a/../b"])
def test_agent_contract_rejects_unsafe_artifact_paths(path: str) -> None:
    with pytest.raises(ValidationError):
        AgentContractRequest(name="agent", output_artifacts=[path])


@pytest.mark.parametrize(
    "artifacts",
    [
        [
            "artifacts/result.json",
            OutputArtifact(
                path="artifacts//result.json",
                source="/logs/optional.json",
                required=False,
            ),
        ],
        [
            OutputArtifact(
                path="telemetry/result.json",
                source="/logs/first.json",
                required=False,
            ),
            OutputArtifact(
                path="telemetry//result.json",
                source="/logs/second.json",
                required=False,
            ),
        ],
    ],
    ids=["required-optional", "optional-optional"],
)
def test_agent_contract_rejects_duplicate_normalized_output_artifact_paths(
    artifacts: list[str | OutputArtifact],
) -> None:
    with pytest.raises(ValidationError, match="duplicate"):
        AgentContractRequest(name="agent", output_artifacts=artifacts)


@pytest.mark.parametrize("source", ["relative/path", "/../escape", "/*.json"])
def test_output_artifact_rejects_unsafe_sources(source: str) -> None:
    with pytest.raises(ValidationError):
        OutputArtifact(path="result.json", source=source)


def test_start_request_matches_canonical_wire_shape() -> None:
    payload = load_fixture("start.json")["request"]
    assert StartBenchmarkRequest.model_validate(payload).model_dump(mode="json") == payload


def test_nested_response_models_ignore_additive_fields() -> None:
    payload = load_fixture("fetch.json")["response"]
    assert isinstance(payload, dict)
    payload["future_top_level"] = True
    details = payload["details"]
    assert isinstance(details, dict)
    details["future_nested"] = {"value": 1}

    response = FetchBenchmarkResponse.model_validate(payload)

    assert response.benchmark_name == "swebench"


def test_non_empty_list_and_final_results_parse() -> None:
    list_response = FetchBenchmarksResponse.model_validate(load_fixture("list.json")["response"])
    result = FinalViewResponse.model_validate(load_fixture("results.json")["inline"])

    assert len(list_response.benchmarks) == 1
    assert result.final_evaluation is not None
    assert result.benchmark_arguments.contract.output_artifacts


def test_final_results_without_environment_parse_as_aws() -> None:
    payload = load_fixture("results.json")["inline"]
    assert isinstance(payload, dict)
    arguments = payload["benchmark_arguments"]
    assert isinstance(arguments, dict)
    del arguments["environment"]

    result = FinalViewResponse.model_validate(payload)

    assert result.benchmark_arguments.environment == "aws"
