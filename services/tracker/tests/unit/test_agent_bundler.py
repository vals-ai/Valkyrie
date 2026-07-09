import zipfile
from io import BytesIO

import pytest

from pydantic import ValidationError

from tracker.agent.bundler import _zip_directory_to_file, get_agent_zip_stream
from tracker.agent.contract import get_contract_from_zip_bytes, read_agent_name
from tracker.agent.schemas import AgentConfig, AgentContract, validate_agent_name
from tracker.exceptions import BundlerError


def test_zip_directory_skips_caches_and_dotfiles(tmp_path):
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


def test_get_agent_zip_stream_wraps_single_file(tmp_path):
    agent_file = tmp_path / "contract.yaml"
    agent_file.write_text("name: file-agent")

    with get_agent_zip_stream("file-agent", agent_file) as stream:
        zip_bytes = stream.read()

    with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
        assert zf.namelist() == ["file-agent/contract.yaml"]
        assert zf.read("file-agent/contract.yaml") == b"name: file-agent"


def test_get_contract_from_zip_bytes_loads_yaml_contract():
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
    assert contract.run_cmd == "python run.py --problem {problem_statement_path} --model gpt-4o"
    assert contract.final_output == "/tmp/final.txt"
    assert contract.output_artifacts[0].path == "logs"
    assert contract.output_artifacts[0].source == "/tmp/logs"
    assert contract.secrets == {"API_KEY": "API key"}


def test_get_contract_from_zip_bytes_reports_missing_contract():
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("agent-a/README.md", "no contract")

    with pytest.raises(BundlerError, match="No contract file found"):
        get_contract_from_zip_bytes("agent-a", zip_buffer.getvalue(), AgentConfig())


def test_validate_agent_name_accepts_valid():
    assert validate_agent_name("my-agent_1.0") == "my-agent_1.0"


def test_validate_agent_name_rejects_invalid():
    with pytest.raises(ValueError):
        validate_agent_name("bad name")


def test_agent_contract_rejects_invalid_name():
    with pytest.raises(ValidationError):
        AgentContract(name="bad name", install_cmd="true", run_cmd="echo {problem_statement_path}")


def test_read_agent_name_from_contract(tmp_path):
    (tmp_path / "contract.yaml").write_text(
        'name: my_agent\ninstall_cmd: bash setup.sh\nrun_cmd: "agent --task {problem_statement_path}"\n'
    )

    assert read_agent_name(tmp_path) == "my_agent"


def test_read_agent_name_missing_contract(tmp_path):
    with pytest.raises(BundlerError, match="No contract file found"):
        read_agent_name(tmp_path)


def test_read_agent_name_malformed_contract(tmp_path):
    (tmp_path / "contract.yaml").write_text("")

    with pytest.raises(BundlerError, match="expected a mapping"):
        read_agent_name(tmp_path)


def test_read_agent_name_missing_required_fields(tmp_path):
    (tmp_path / "contract.yaml").write_text("name: my_agent\n")

    with pytest.raises(BundlerError, match="Invalid contract file"):
        read_agent_name(tmp_path)
