"""Contract bundler for creating uploadable bundles."""

import os
import shutil
import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Generator

import tomli_w


class BundlerError(Exception):
    """Exception raised for bundler errors."""

    pass


# Minimal pyproject.toml for sandbox deployment
MINIMAL_PYPROJECT = {
    "project": {
        "name": "agentic-harness",
        "version": "0.1.0",
        "description": "An agentic harness that interfaces with benchmarks hosted by Vals AI",
        "requires-python": ">=3.10",
        "dependencies": [
            "pypandoc",
            "pytest>=9.0.1",
            "pytest-asyncio>=1.3.0",
            "daytona>=0.123.0",
            "click>=8.1.0",
            "wonderwords>=3.0.1",
            "httpx>=0.28.1",
            "python-dotenv",
            "pydantic>=2.0",
        ],
    },
    "build-system": {
        "requires": ["hatchling"],
        "build-backend": "hatchling.build",
    },
}


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
def create_contract_bundle_stream(contract_path: Path) -> Generator[BinaryIO, None, None]:
    """
    Create bundle containing agentic_harness package and contract, return file stream.

    Creates a bundle with:
    - agentic_harness package
    - minimal pyproject.toml for sandbox
    - contracts/{contract_name}/

    Args:
        contract_path: Path to contract directory

    Yields:
        Tuple of (file_handle, contract_name) for streaming upload

    Raises:
        BundlerError: If required files/directories are not found
    """
    if not contract_path.exists():
        raise BundlerError(f"Contract directory not found at {contract_path}")

    contract_name = contract_path.name
    project_root = contract_path.parent.parent

    # Create unique temporary directory for bundling
    bundle_path = Path(tempfile.mkdtemp(prefix="bundle_"))

    # Create temporary zip file
    zip_file = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    zip_path = Path(zip_file.name)
    zip_file.close()

    ignore_patterns = shutil.ignore_patterns("__pycache__", "*.pyc")

    try:
        # Copy agentic_harness package from source to bundle
        source_harness_path = project_root / "src" / "agentic_harness"
        bundle_harness_path = bundle_path / "agentic_harness"
        if not source_harness_path.exists():
            raise BundlerError(f"agentic_harness package not found at {source_harness_path}")
        shutil.copytree(source_harness_path, bundle_harness_path, ignore=ignore_patterns)

        # Write minimal pyproject.toml for sandbox
        bundle_pyproject_path = bundle_path / "pyproject.toml"
        with open(bundle_pyproject_path, "wb") as f:
            tomli_w.dump(MINIMAL_PYPROJECT, f)

        # Create contracts/__init__.py in bundle
        bundle_contracts_dir = bundle_path / "contracts"
        bundle_contracts_dir.mkdir(exist_ok=True)
        (bundle_contracts_dir / "__init__.py").touch()

        # Copy contract directory from source to bundle
        bundle_contract_path = bundle_contracts_dir / contract_name
        shutil.copytree(contract_path, bundle_contract_path, ignore=ignore_patterns)

        # Zip the bundle to file
        _zip_directory_to_file(bundle_path, zip_path)

        with open(zip_path, "rb") as f:
            yield f
    finally:
        shutil.rmtree(bundle_path, ignore_errors=True)
        zip_path.unlink(missing_ok=True)


def validate_contract(contract_path: Path) -> list[str]:
    """
    Validate contract structure.

    Args:
        contract_path: Path to contract directory

    Returns:
        List of validation error messages. Empty list if valid.
    """
    errors: list[str] = []

    # Check that contract.py exists
    contract_file = contract_path / "contract.py"
    if not contract_file.exists():
        errors.append("Contract directory must contain a contract.py file")

    # TODO: Add more validation:
    # - Check if it imports/implements AgentContract
    # - Validate submodule is a pyproject
    # - Validate setup.sh if present

    return errors
