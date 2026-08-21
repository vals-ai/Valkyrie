"""Tests for agent contracts and the CLI push command.

Run: uv run pytest tests/unit/cli/agent/test_contract.py
"""

from pathlib import Path
from textwrap import dedent
from typing import Any, cast

import pytest
from click.testing import CliRunner
from pydantic import ValidationError
from tracker.agent.schemas import AgentConfig, AgentContract, Parameter
from tracker.database.models import AgentContractRequest, OutputArtifact

from tracker.agent.contract import _parse_yaml_contract  # type: ignore
from valkyrie.cli.main import agent


def _make_contract(**overrides: Any) -> AgentContract:
    defaults: dict[str, Any] = {
        "name": "test_agent",
        "install_cmd": "bash setup.sh",
        "run_cmd": "agent --task {problem_statement_path}",
    }
    return AgentContract(**{**defaults, **overrides})


class TestValidateKwargs:
    def test_defaults_applied_when_no_values(self) -> None:
        """
        Validates that defaults are applied when no value is provided.

        Test Cases:
        - Temperature has a default of 0.7 and the user provides no kwargs
        """
        contract = _make_contract(
            kwargs={
                "temperature": Parameter(type="float", required=False, default=0.7),
            }
        )

        result = contract.validate_kwargs(contract.kwargs, {})

        assert result == {"temperature": 0.7}

    def test_override_replaces_default(self) -> None:
        """
        Validates that a user-provided value overrides the schema default.

        Test Cases:
        - Temperature defaults to 0.7 but user passes 1.0 via kwargs
        """
        contract = _make_contract(
            kwargs={
                "temperature": Parameter(type="float", required=False, default=0.7),
            }
        )

        result = contract.validate_kwargs(contract.kwargs, {"temperature": 1.0})

        assert result == {"temperature": 1.0}

    def test_required_kwarg_missing_raises(self) -> None:
        """
        Validates that a missing required kwarg raises a ValidationError.

        Test Cases:
        - Model is required but the user provides no kwargs
        """
        contract = _make_contract(
            kwargs={
                "model": Parameter(type="str", required=True),
            }
        )

        with pytest.raises(ValueError):
            contract.validate_kwargs(contract.kwargs, {})

    def test_required_kwarg_provided(self) -> None:
        """
        Validates that a required kwarg passes when the user provides it.

        Test Cases:
        - Model is required and the user provides "gpt-4o"
        """
        contract = _make_contract(
            kwargs={
                "model": Parameter(type="str", required=True),
            }
        )

        result = contract.validate_kwargs(contract.kwargs, {"model": "gpt-4o"})

        assert result == {"model": "gpt-4o"}

    def test_wrong_type_raises(self) -> None:
        """
        Validates that passing a value with the wrong type raises a ValidationError.

        Test Cases:
        - Temperature expects a float but the user provides a string
        """
        contract = _make_contract(
            kwargs={
                "temperature": Parameter(type="float", required=False, default=0.7),
            }
        )

        with pytest.raises(ValueError):
            contract.validate_kwargs(contract.kwargs, {"temperature": "not_a_float"})

    def test_empty_schema_returns_empty(self) -> None:
        """
        Validates that an empty kwargs schema returns an empty dict.

        Test Cases:
        - Contract has no kwargs defined, values are ignored
        """
        contract = _make_contract()

        result = contract.validate_kwargs({}, {"ignored": "value"})

        assert result == {}

    def test_multiple_kwargs_mixed(self) -> None:
        """
        Validates that defaults, required values, and overrides work together.

        Test Cases:
        - Temperature and verbose have defaults, model is required and provided by the user
        """
        contract = _make_contract(
            kwargs={
                "temperature": Parameter(type="float", required=False, default=0.7),
                "model": Parameter(type="str", required=True),
                "verbose": Parameter(type="bool", required=False, default=True),
            }
        )

        result = contract.validate_kwargs(contract.kwargs, {"model": "gpt-4o"})

        assert result == {"temperature": 0.7, "model": "gpt-4o", "verbose": True}

    def test_choices_enforced(self) -> None:
        """
        Validates that an invalid choice raises a ValueError.

        Test Cases:
        - Mode has choices ["fast", "accurate"] and "invalid" is rejected
        """
        contract = _make_contract(
            kwargs={
                "mode": Parameter(type="str", required=False, default="fast", choices=["fast", "accurate"]),
            }
        )

        with pytest.raises(ValueError):
            contract.validate_kwargs(contract.kwargs, {"mode": "invalid"})

    def test_choices_valid(self) -> None:
        """
        Validates that a valid choice is accepted.

        Test Cases:
        - Mode has choices ["fast", "accurate"] and "fast" is accepted
        """
        contract = _make_contract(
            kwargs={
                "mode": Parameter(type="str", required=False, default="fast", choices=["fast", "accurate"]),
            }
        )

        result = contract.validate_kwargs(contract.kwargs, {"mode": "fast"})

        assert result == {"mode": "fast"}


class TestFormatRunCmd:
    def test_kwargs_substituted(self) -> None:
        """
        Validates that kwargs are substituted into the run command string.

        Test Cases:
        - {temperature} placeholder is replaced with the provided value
        """
        contract = _make_contract(
            run_cmd="agent --task {problem_statement_path} --temp {temperature}",
        )

        result = contract.format_run_cmd({"temperature": 0.7})

        assert result == "agent --task {problem_statement_path} --temp 0.7"

    def test_defaults_preserved(self) -> None:
        """
        Validates that built-in placeholders are preserved after formatting.

        Test Cases:
        - {problem_statement_path} remains in the run command for runtime substitution
        """
        contract = _make_contract()

        result = contract.format_run_cmd({})

        assert "{problem_statement_path}" in result

    def test_multiple_kwargs(self) -> None:
        """
        Validates that user kwargs and built-in placeholders coexist in the run command.

        Test Cases:
        - {temperature} is substituted, {problem_statement_path} and {task_id} are preserved
        """
        contract = _make_contract(
            run_cmd="agent --task {problem_statement_path} --temp {temperature} --id {task_id}",
        )

        result = contract.format_run_cmd({"temperature": 0.5})

        assert result == "agent --task {problem_statement_path} --temp 0.5 --id {task_id}"


class TestOutputArtifactValidation:
    def test_accepts_relative_destination_path(self) -> None:
        request = AgentContractRequest(
            name="test-agent",
            install_cmd="true",
            run_cmd="echo {problem_statement_path}",
            output_artifacts=["metrics/result.json"],
        )

        assert request.output_artifacts == ["metrics/result.json"]

    def test_accepts_single_component_relative_destination(self) -> None:
        request = AgentContractRequest(
            name="test-agent",
            install_cmd="true",
            run_cmd="echo {problem_statement_path}",
            output_artifacts=["result.json"],
        )

        assert request.output_artifacts == ["result.json"]

    def test_rejects_path_traversal(self) -> None:
        with pytest.raises(ValidationError, match="output_artifacts"):
            AgentContractRequest(
                name="test-agent",
                install_cmd="true",
                run_cmd="echo {problem_statement_path}",
                output_artifacts=["artifacts/../secret.json"],
            )

    def test_rejects_absolute_path(self) -> None:
        with pytest.raises(ValidationError, match="relative"):
            AgentContractRequest(
                name="test-agent",
                install_cmd="true",
                run_cmd="echo {problem_statement_path}",
                output_artifacts=["/tmp/artifacts/result.json"],
            )

    def test_rejects_too_many_output_artifacts(self) -> None:
        with pytest.raises(ValidationError, match="output_artifacts"):
            AgentContractRequest(
                name="test-agent",
                install_cmd="true",
                run_cmd="echo {problem_statement_path}",
                output_artifacts=["artifacts/result.json"] * 11,
            )

    def test_empty_source_is_treated_as_default_source(self) -> None:
        artifact = OutputArtifact(path="artifacts/result.json", source="")

        assert artifact.source is None

    def test_rejects_relative_source_path(self) -> None:
        with pytest.raises(ValidationError, match="absolute sandbox paths"):
            OutputArtifact(path="artifacts/result.json", source="logs/result.json")

    def test_rejects_root_glob_source_path(self) -> None:
        with pytest.raises(ValidationError, match="non-root directory prefix"):
            OutputArtifact(path="artifacts/result.json", source="/*.json")

    def test_accepts_explicit_source_and_destination(self) -> None:
        request = AgentContractRequest(
            name="test-agent",
            install_cmd="true",
            run_cmd="echo {problem_statement_path}",
            output_artifacts=[
                OutputArtifact(
                    path="artifacts/result.json",
                    source="/logs/{task_id}/result.json",
                )
            ],
        )

        artifact = request.output_artifacts[0]
        assert not isinstance(artifact, str)
        assert artifact.path == "artifacts/result.json"
        assert artifact.source == "/logs/{task_id}/result.json"


class TestRunCmdValidation:
    def test_missing_problem_statement_path_raises(self) -> None:
        """
        Validates that run_cmd without {problem_statement_path} raises a ValidationError.

        Test Cases:
        - run_cmd has no placeholder for the problem statement path
        """
        with pytest.raises(ValidationError, match="problem_statement_path"):
            _make_contract(run_cmd="agent --no-placeholder")

    def test_valid_run_cmd(self) -> None:
        """
        Validates that a run_cmd containing {problem_statement_path} passes validation.

        Test Cases:
        - run_cmd includes the required placeholder
        """
        contract = _make_contract(run_cmd="agent {problem_statement_path}")
        assert contract.run_cmd == "agent {problem_statement_path}"


class TestParseYamlContract:
    """End-to-end: YAML file -> AgentContractRequest with kwargs resolved."""

    def _write_yaml(self, tmp_path: Path, content: str) -> Path:
        p = tmp_path / "contract.yaml"
        p.write_text(dedent(content))
        return p

    def test_defaults_applied(self, tmp_path: Path) -> None:
        """
        Validates that YAML kwargs defaults are resolved into the run command.

        Test Cases:
        - Temperature defaults to 0.7 and appears in the final AgentContractRequest run_cmd
        """
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --temp {temperature}"
            kwargs:
              temperature:
                type: float
                required: false
                default: 0.7
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig())

        assert isinstance(result, AgentContractRequest)
        assert "--temp 0.7" in result.run_cmd
        assert "{problem_statement_path}" in result.run_cmd
        assert result.kwargs == {"temperature": "0.7"}

    def test_cli_kwargs_override_defaults(self, tmp_path: Path) -> None:
        """
        Validates that CLI kwargs (-k) override YAML defaults in the final run command.

        Test Cases:
        - Temperature defaults to 0.7 in YAML but user passes 1.0 via AgentConfig
        """
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --temp {temperature}"
            kwargs:
              temperature:
                type: float
                required: false
                default: 0.7
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig(kwargs={"temperature": 1.0}))

        assert "--temp 1.0" in result.run_cmd
        assert result.kwargs == {"temperature": "1.0"}

    def test_required_kwarg_missing_raises(self, tmp_path: Path) -> None:
        """
        Validates that a missing required kwarg raises during YAML contract parsing.

        Test Cases:
        - Model is required in the YAML schema but not provided via AgentConfig
        """
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --model {model}"
            kwargs:
              model:
                type: str
                required: true
        """,
        )

        with pytest.raises(ValueError, match="is required but was not provided"):
            _parse_yaml_contract(path, AgentConfig())

    def test_secrets_final_output_and_output_artifacts_passed_through(self, tmp_path: Path) -> None:
        """
        Validates that secrets, final_output, and output_artifacts from YAML are carried to the request.

        Test Cases:
        - YAML defines API_KEY secret, /artifacts final_output, and one direct output artifact
        """
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path}"
            final_output: /artifacts
            output_artifacts:
              - artifacts/turns.jsonl
              - path: artifacts/result.json
                source: /logs/{task_id}/result.json
            egress_allowlist:
              - https://api.openai.com
              - https://github.com
            secrets:
              API_KEY: MySecretName
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig())

        assert result.final_output == "/artifacts"
        assert result.output_artifacts[0] == "artifacts/turns.jsonl"
        artifact = cast(OutputArtifact, result.output_artifacts[1])
        assert artifact.path == "artifacts/result.json"
        assert artifact.source == "/logs/{task_id}/result.json"
        assert result.egress_allowlist == ["https://api.openai.com", "https://github.com"]
        assert result.secrets == {"API_KEY": "MySecretName"}

    def test_model_from_agent_config(self, tmp_path: Path) -> None:
        """
        Validates that the model comes from AgentConfig, not the YAML contract.

        Test Cases:
        - AgentConfig specifies "gpt-4o", YAML has no model field
        """
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path}"
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig(model="gpt-4o"))

        assert result.model == "gpt-4o"

    def test_no_kwargs_in_schema(self, tmp_path: Path) -> None:
        """
        Validates that a YAML contract with no kwargs produces an unmodified run command.

        Test Cases:
        - YAML defines no kwargs, run_cmd passes through with only built-in placeholders
        """
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path}"
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig())

        assert result.run_cmd == "agent --task {problem_statement_path}"

    def test_defaults_model_from_cli(self, tmp_path: Path) -> None:
        """
        Validates that --model is injected into defaults and substituted into run_cmd.

        Test Cases:
        - Model is defined in defaults as required, user passes --model on the CLI
        """
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --model {model}"
            defaults:
              model:
                type: str
                required: true
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig(model="gpt-4o"))

        assert "--model gpt-4o" in result.run_cmd

    def test_defaults_model_required_missing_raises(self, tmp_path: Path) -> None:
        """
        Validates that a required default raises when --model is not provided.

        Test Cases:
        - Model is required in defaults but no --model flag is passed
        """
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --model {model}"
            defaults:
              model:
                type: str
                required: true
        """,
        )

        with pytest.raises(ValueError, match="is required but was not provided"):
            _parse_yaml_contract(path, AgentConfig())

    def test_defaults_merged_with_kwargs(self, tmp_path: Path) -> None:
        """
        Validates that defaults and kwargs are merged and both substituted into run_cmd.

        Test Cases:
        - Model comes from defaults via --model, temperature comes from kwargs default
        """
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path} --model {model} --temp {temperature}"
            defaults:
              model:
                type: str
                required: true
            kwargs:
              temperature:
                type: float
                required: false
                default: 0.7
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig(model="gpt-4o"))

        assert "--model gpt-4o" in result.run_cmd
        assert "--temp 0.7" in result.run_cmd

    def test_provided_section_ignored(self, tmp_path: Path) -> None:
        """
        Validates that the provided section is stripped and does not cause a parse error.

        Test Cases:
        - YAML includes a provided section for documentation, it is silently ignored
        """
        path = self._write_yaml(
            tmp_path,
            """\
            name: my_agent
            install_cmd: bash setup.sh
            run_cmd: "agent --task {problem_statement_path}"
            provided:
              task_id:
                type: str
                required: false
              problem_statement_path:
                type: str
                required: true
        """,
        )

        result = _parse_yaml_contract(path, AgentConfig())

        assert result.name == "my_agent"


class TestPushCommand:
    def _write_contract(self, agent_dir: Path) -> None:
        (agent_dir / "contract.yaml").write_text(
            dedent(
                """\
                name: my_agent
                install_cmd: bash setup.sh
                run_cmd: "agent --task {problem_statement_path}"
                """
            )
        )

    def test_push_uses_contract_name(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self._write_contract(tmp_path)
        pushed: dict[str, str] = {}

        async def mock_push(agent_name: str, agent_path: Path) -> None:
            pushed["name"] = agent_name

        monkeypatch.setattr("valkyrie.cli.agent.lifecycle.push_agent", mock_push)

        result = CliRunner().invoke(agent, ["push", str(tmp_path)])

        assert result.exit_code == 0
        assert pushed["name"] == "my_agent"

    def test_push_name_flag_overrides_contract_name(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self._write_contract(tmp_path)
        pushed: dict[str, str] = {}

        async def mock_push(agent_name: str, agent_path: Path) -> None:
            pushed["name"] = agent_name

        monkeypatch.setattr("valkyrie.cli.agent.lifecycle.push_agent", mock_push)

        result = CliRunner().invoke(agent, ["push", str(tmp_path), "--name", "override"])

        assert result.exit_code == 0
        assert pushed["name"] == "override"
