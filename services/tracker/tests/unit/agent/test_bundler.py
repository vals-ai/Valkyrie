"""Unit tests for agent bundle creation and validation.

Run: uv run pytest tests/unit/agent/test_bundler.py
"""

import zipfile
from io import BytesIO
from pathlib import Path

import pytest

from pydantic import ValidationError

from tracker.agent.contract import get_contract_from_zip_bytes, read_agent_name
from tracker.agent.schemas import AgentConfig, AgentContract, OutputArtifact, validate_agent_name
from tracker.exceptions import BundlerError


class TestGetContractFromZipBytes:
    """Contract loading from agent zip archives."""

    @pytest.mark.parametrize("agent_name", ["agent-a", "library-alias"])
    def test_get_contract_from_zip_bytes_loads_yaml_contract(self, agent_name: str) -> None:
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr(
                f"{agent_name}/contract.yaml",
                """
name: agent-a
install_cmd: echo install
run_cmd: python run.py --problem {problem_statement_path} --model {model}
kwargs:
  model:
    type: str
    required: true
final_output: /tmp/final.txt
output_artifacts:
  - path: logs
    source: /tmp/logs
    required: false
secrets:
  API_KEY: API key
""",
            )

        contract = get_contract_from_zip_bytes(agent_name, zip_buffer.getvalue(), AgentConfig(model="gpt-4o"))

        assert contract.name == agent_name
        assert contract.model == "gpt-4o"
        assert contract.install_cmd == "echo install"
        assert contract.run_cmd == "python run.py --problem {problem_statement_path} --model gpt-4o"
        assert contract.final_output == "/tmp/final.txt"
        output_artifact = contract.output_artifacts[0]
        assert output_artifact == OutputArtifact(
            path="logs",
            source="/tmp/logs",
            required=False,
        )

        serialized_artifact = contract.model_dump(mode="json")["output_artifacts"][0]
        assert serialized_artifact["required"] is False
        assert contract.secrets == {"API_KEY": "API key"}

    def test_get_contract_from_zip_bytes_reports_missing_contract(self) -> None:
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("agent-a/README.md", "no contract")

        with pytest.raises(BundlerError, match="No contract file found"):
            get_contract_from_zip_bytes("agent-a", zip_buffer.getvalue(), AgentConfig())


class TestAgentNameValidation:
    """Agent name validation for bundles and contracts."""

    def test_validate_agent_name_accepts_valid(self) -> None:
        assert validate_agent_name("my-agent_1.0") == "my-agent_1.0"

    def test_validate_agent_name_rejects_invalid(self) -> None:
        with pytest.raises(ValueError):
            validate_agent_name("bad name")

    def test_agent_contract_rejects_invalid_name(self) -> None:
        with pytest.raises(ValidationError):
            AgentContract(name="bad name", install_cmd="true", run_cmd="echo {problem_statement_path}")


class TestReadAgentNameFromContract:
    """Agent name loading from contract files."""

    def test_read_agent_name_from_contract(self, tmp_path: Path) -> None:
        (tmp_path / "contract.yaml").write_text(
            'name: my_agent\ninstall_cmd: bash setup.sh\nrun_cmd: "agent --task {problem_statement_path}"\n'
        )

        assert read_agent_name(tmp_path) == "my_agent"

    def test_read_agent_name_missing_contract(self, tmp_path: Path) -> None:
        with pytest.raises(BundlerError, match="No contract file found"):
            read_agent_name(tmp_path)

    def test_read_agent_name_malformed_contract(self, tmp_path: Path) -> None:
        (tmp_path / "contract.yaml").write_text("")

        with pytest.raises(BundlerError, match="expected a mapping"):
            read_agent_name(tmp_path)

    def test_read_agent_name_missing_required_fields(self, tmp_path: Path) -> None:
        (tmp_path / "contract.yaml").write_text("name: my_agent\n")

        with pytest.raises(BundlerError, match="Invalid contract file"):
            read_agent_name(tmp_path)
