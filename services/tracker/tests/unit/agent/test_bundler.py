"""Unit tests for agent bundle creation and validation.

Run: uv run pytest tests/unit/agent/test_bundler.py
"""

import zipfile
import subprocess
from io import BytesIO
from pathlib import Path

import pytest

from pydantic import ValidationError

import tracker.agent.bundler as bundler_module
from tracker.agent.bundler import get_agent_zip_stream
from tracker.agent.contract import get_contract_from_zip_bytes, read_agent_name
from tracker.agent.schemas import (
    AgentConfig,
    AgentContract,
    OutputArtifact,
    bind_shell_variables,
    prepare_shell_command,
    validate_agent_name,
)
from tracker.exceptions import BundlerError

_zip_directory_to_file = getattr(bundler_module, "_zip_directory_to_file")


def test_zip_directory_skips_caches_and_dotfiles(tmp_path: Path) -> None:
    agent_dir = tmp_path / "agent"
    agent_dir.mkdir()
    (agent_dir / "run.py").write_text("print('ok')")
    (agent_dir / ".env").write_text("SECRET=value")
    cache_dir = agent_dir / "__pycache__"
    cache_dir.mkdir()
    (cache_dir / "run.cpython-312.pyc").write_bytes(b"cache")

    zip_path = tmp_path / "agent.zip"

    _zip_directory_to_file(agent_dir, zip_path)

    with zipfile.ZipFile(zip_path) as zf:
        assert zf.namelist() == ["agent/run.py"]


def test_get_agent_zip_stream_wraps_single_file(tmp_path: Path) -> None:
    agent_file = tmp_path / "contract.yaml"
    agent_file.write_text("name: file-agent")

    with get_agent_zip_stream("file-agent", agent_file) as stream:
        zip_bytes = stream.read()

    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        assert zf.namelist() == ["file-agent/contract.yaml"]
        assert zf.read("file-agent/contract.yaml") == b"name: file-agent"


class TestGetContractFromZipBytes:
    """Contract loading from agent zip archives."""

    def test_get_contract_from_zip_bytes_loads_yaml_contract(self) -> None:
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr(
                "agent-a/contract.yaml",
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
secrets:
  API_KEY: API key
""",
            )

        contract = get_contract_from_zip_bytes("agent-a", zip_buffer.getvalue(), AgentConfig(model="gpt-4o"))

        assert contract.name == "agent-a"
        assert contract.model == "gpt-4o"
        assert contract.install_cmd == "echo install"
        assert contract.run_cmd.count("export VALKYRIE_ARG_") == 1
        assert '--problem "${VALKYRIE_ARG_' in contract.run_cmd
        assert '--model "${VALKYRIE_ARG_' in contract.run_cmd
        assert "=gpt-4o" in contract.run_cmd
        assert contract.final_output == "/tmp/final.txt"
        output_artifact = contract.output_artifacts[0]
        assert isinstance(output_artifact, OutputArtifact)
        assert output_artifact.path == "logs"
        assert output_artifact.source == "/tmp/logs"
        assert contract.secrets == {"API_KEY": "API key"}

    def test_get_contract_from_zip_bytes_reports_missing_contract(self) -> None:
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("agent-a/README.md", "no contract")

        with pytest.raises(BundlerError, match="No contract file found"):
            get_contract_from_zip_bytes("agent-a", zip_buffer.getvalue(), AgentConfig())

    def test_get_contract_from_zip_bytes_shell_quotes_arguments(self) -> None:
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr(
                "agent-a/contract.yaml",
                """
name: agent-a
install_cmd: echo install
run_cmd: python run.py --problem {problem_statement_path} --model {model}
kwargs:
  model:
    type: str
    required: true
""",
            )

        contract = get_contract_from_zip_bytes(
            "agent-a",
            zip_buffer.getvalue(),
            AgentConfig(model="model; printenv"),
        )

        assert contract.run_cmd.count("export VALKYRIE_ARG_") == 1
        assert "='model; printenv'" in contract.run_cmd
        assert '--model "${VALKYRIE_ARG_' in contract.run_cmd

    def test_get_contract_from_zip_bytes_rejects_quoted_placeholders(self) -> None:
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr(
                "agent-a/contract.yaml",
                """
name: agent-a
install_cmd: echo install
run_cmd: 'python run.py --problem {problem_statement_path} --model "{model}"'
kwargs:
  model:
    type: str
    required: true
""",
            )

        with pytest.raises(BundlerError, match="must be a standalone shell argument"):
            get_contract_from_zip_bytes(
                "agent-a",
                zip_buffer.getvalue(),
                AgentConfig(model='model"; printenv; #'),
            )


def test_shell_arguments_are_data(tmp_path: Path) -> None:
    for index, value in enumerate(
        (
            "$(touch marker)",
            "`touch marker`",
            "value; touch marker",
            'value"; touch marker; #',
        )
    ):
        marker = tmp_path / f"marker-{index}"
        payload = value.replace("marker", str(marker))
        template = prepare_shell_command("printf '%s\\n' {model}", ["model"])
        command = bind_shell_variables(template, {"model": payload})

        result = subprocess.run(
            ["/bin/sh", "-c", command],
            check=True,
            capture_output=True,
            text=True,
        )

        assert result.stdout == f"{payload}\n"
        assert not marker.exists()


@pytest.mark.parametrize(
    "command",
    [
        "cat <<EOF\n{model}\nEOF",
        "eval {model}",
        "sh -c {model}",
        "echo $( {model} )",
        "echo `{model}`",
    ],
)
def test_dynamic_arguments_reject_shell_reinterpretation(command: str) -> None:
    with pytest.raises(ValueError):
        prepare_shell_command(command, ["model"])


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
