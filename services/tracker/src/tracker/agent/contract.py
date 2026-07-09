"""Resolve agent contracts from yaml files / zipped agent bundles."""

import io
import tempfile
import zipfile
from pathlib import Path

import yaml
from pydantic import ValidationError as PydanticValidationError

from tracker.agent.schemas import AgentConfig, AgentContract
from tracker.database.models import AgentContractRequest
from tracker.exceptions import BundlerError, ContractValidationError


def read_agent_name(agent_dir: Path) -> str:
    """Read and validate the agent name from a directory's contract.yaml/.yml."""
    for ext in (".yaml", ".yml"):
        contract_file = agent_dir / f"contract{ext}"
        if contract_file.exists():
            data = yaml.safe_load(contract_file.read_text())
            if not isinstance(data, dict):
                raise BundlerError(f"Invalid contract file '{contract_file}': expected a mapping")
            try:
                return AgentContract(**data).name
            except PydanticValidationError as e:
                raise BundlerError(f"Invalid contract file '{contract_file}': {e}") from e

    raise BundlerError(f"No contract file found in agent directory '{agent_dir}'")


def get_contract_from_zip_bytes(agent_name: str, zip_bytes: bytes, agent_config: AgentConfig) -> AgentContractRequest:
    """Extract contract from zip bytes into a temp dir and load it. Looks for contract.yaml/.yml."""
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                names = zf.namelist()
                for ext in (".yaml", ".yml"):
                    contract_member = f"{agent_name}/contract{ext}"
                    if contract_member in names:
                        zf.extract(contract_member, tmp_path)
                        return get_contract(tmp_path / contract_member, agent_config)

            raise BundlerError(f"No contract file found in zip for agent '{agent_name}'")
    except BundlerError:
        raise
    except Exception as e:
        raise BundlerError(f"Failed to load contract from zip for agent '{agent_name}':\n{e}") from e


def _parse_yaml_contract(contract_path: Path, agent_config: AgentConfig) -> AgentContractRequest:
    """Parse a declarative agent.yaml and validate kwargs against its schema."""
    try:
        with open(contract_path, "r") as f:
            contract_dict = yaml.safe_load(f)

        contract_dict.pop("provided", None)

        try:
            agent_contract = AgentContract(**contract_dict)
        except PydanticValidationError as e:
            contract_name = "/".join(contract_path.parts[-2:])
            raise ContractValidationError(e, context=f"Invalid contract `{contract_name}`") from e

        all_schema = {**agent_contract.defaults, **agent_contract.kwargs}

        user_values = {**agent_config.kwargs}
        if agent_config.model and "model" not in user_values:
            user_values["model"] = agent_config.model

        validated_kwargs = agent_contract.validate_kwargs(all_schema, user_values)

        return AgentContractRequest(
            name=agent_contract.name,
            model=agent_config.model,
            run_cmd=agent_contract.format_run_cmd(validated_kwargs),
            install_cmd=agent_contract.install_cmd,
            final_output=str(agent_contract.final_output) if agent_contract.final_output is not None else None,
            output_artifacts=agent_contract.output_artifacts,
            secrets=agent_contract.secrets,
        )
    except ContractValidationError:
        raise
    except Exception as e:
        raise ValueError(f"Failed to parse YAML contract from `{'/'.join(contract_path.parts[-2:])}`:\n{e}") from e


def get_contract(contract_path: Path, agent_config: AgentConfig) -> AgentContractRequest:
    """Dispatch a contract file to the right parser based on extension (.yaml/.yml)."""
    try:
        match contract_path.suffix:
            case ".yaml" | ".yml":
                return _parse_yaml_contract(contract_path, agent_config)
            case _:
                raise ValueError(f"Unsupported contract format: `{contract_path.suffix}`. Expected '.yaml' or '.yml'")
    except ValueError:
        raise
    except Exception as e:
        raise BundlerError(f"Failed to get contract from `{contract_path}`: {e}") from e
