"""Contract bundler for creating uploadable bundles."""

import importlib.util
import os
import shutil
import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Generator

from tracker.database.models import AgentContractRequest

from agentic_harness.cli.exceptions import BundlerError
from agentic_harness.contract import BaseAgentContract
from agentic_harness.schemas import AgentConfig


def _zip_directory_to_file(directory: Path, output_path: Path) -> None:
    """
    Zip a directory to a file.

    Args:
        directory: Directory to zip
        output_path: Path where zip file will be written

    Raises:
        BundlerError: If zipping fails
    """
    exclude_patterns = {
        "__pycache__",
        ".pyc",
        ".pyo",
        ".pyd",
        ".so",
        ".dll",
        ".dylib",
        ".egg-info",
        ".git",
        ".venv",
        "venv",
        ".env",
        ".DS_Store",
    }

    try:
        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(directory):
                dirs[:] = [d for d in dirs if d not in exclude_patterns]

                for file in files:
                    if any(pattern in file for pattern in exclude_patterns):
                        continue

                    file_path = Path(root) / file
                    arcname = file_path.relative_to(directory.parent)
                    zipf.write(file_path, arcname)
    except (OSError, zipfile.BadZipFile) as e:
        raise BundlerError(f"Failed to create zip archive: {e}") from e


@contextmanager
def get_agent_zip_stream(contract: AgentContractRequest) -> Generator[BinaryIO, None, None]:
    """
    Create a zip stream containing the agent artifacts.

    Args:
        contract: AgentContractRequest instance

    Returns:
        Generator[BinaryIO, None, None]: A generator that yields a zip stream

    Raises:
        BundlerError: If any artifacts are missing or zipping fails
    """
    agent_path = Path("agents") / contract.name
    artifacts = [agent_path / artifact for artifact in contract.artifacts]

    missing_artifacts = [artifact for artifact in artifacts if not artifact.exists()]
    if missing_artifacts:
        raise BundlerError(f"Missing artifacts: {missing_artifacts}")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        bundle_dir = temp_path / contract.name
        bundle_dir.mkdir(parents=True, exist_ok=True)

        for artifact in artifacts:
            dest = bundle_dir / artifact.relative_to(agent_path)
            if artifact.is_dir():
                shutil.copytree(artifact, dest)
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(artifact, dest)

        zip_path = temp_path / f"{contract.name}.zip"
        _zip_directory_to_file(bundle_dir, zip_path)

        with open(zip_path, "rb") as f:
            yield f


def get_contract(contract_path: Path, agent_config: AgentConfig) -> AgentContractRequest:
    try:
        spec = importlib.util.spec_from_file_location("contract", contract_path)

        if not spec or not spec.loader:
            raise ImportError(f"Failed to import contract from {contract_path}")

        module = importlib.util.module_from_spec(spec)

        spec.loader.exec_module(module)

        Contract: type[BaseAgentContract] = module.contract

        contract = Contract(agent_config)

        return contract.to_request()
    except Exception as e:
        raise BundlerError(f"Failed to get contract from {contract_path}: {e}") from e
