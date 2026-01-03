"""Utility functions for the CLI.

TODO: Create TrackerService class for accessing tracker endpoints.
"""

import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import click
import httpx
import tomli_w

from agentic_harness.cli.config import TRACKER_URL
from agentic_harness.utils import validate_contract as validate_contract_core

# Minimal pyproject.toml for sandbox deployment
MINIMAL_PYPROJECT = {
    "project": {
        "name": "agentic-harness",
        "version": "0.1.0",
        "description": "An agentic harness that interfaces with benchmarks hosted by Vals AI",
        "requires-python": ">=3.11",
        "dependencies": [
            "pypandoc",
            "pytest>=9.0.1",
            "pytest-asyncio>=1.3.0",
            "daytona>=0.123.0",
            "click>=8.1.0",
            "wonderwords>=3.0.1",
            "model-library",
            "httpx>=0.28.1",
        ],
    },
    "build-system": {
        "requires": ["hatchling"],
        "build-backend": "hatchling.build",
    },
}


def validate_contract(contract_path: Path) -> None:
    """
    CLI wrapper for contract validation.

    Args:
        contract_path: Path to contract directory

    Raises:
        click.ClickException: If validation fails
    """
    errors = validate_contract_core(contract_path)
    if errors:
        error_message = "Contract validation failed:\n" + "\n".join(f"  - {error}" for error in errors)
        raise click.ClickException(error_message)


def zip_directory(directory: Path) -> bytes:
    """
    Zip a directory and return the zip file content as bytes.
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

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(directory):
                # Filter out excluded directories
                dirs[:] = [d for d in dirs if d not in exclude_patterns]

                for file in files:
                    # Skip excluded file patterns
                    if any(pattern in file for pattern in exclude_patterns):
                        continue

                    file_path = Path(root) / file
                    arcname = file_path.relative_to(directory.parent)
                    zipf.write(file_path, arcname)

        # Read the zip file content
        with open(tmp_path, "rb") as f:
            return f.read()
    finally:
        # Clean up temporary file
        tmp_path.unlink(missing_ok=True)


def start_benchmark_run(contract_name: str, benchmark_name: str) -> None:
    """
    Start a benchmark run on the tracker service.

    Args:
        agent_name: Name of the agent
        contract_name: Name of the contract
        benchmark_name: Name of the benchmark

    Raises:
        click.ClickException: If request fails
    """
    START_RUN_TIMEOUT = 120

    start_run_url = f"{TRACKER_URL}/start-run"
    payload = {
        "contract_name": contract_name,
        "benchmark_name": benchmark_name,
    }

    try:
        with httpx.Client(timeout=START_RUN_TIMEOUT) as client:
            response = client.post(start_run_url, json=payload)
    except httpx.RequestError as e:
        raise click.ClickException(f"Failed to connect to tracker service: {str(e)}")

    if response.status_code != 200:
        raise click.ClickException(f"Failed to start run: {response}")

    result = response.json()
    click.echo(f"{result.get('message', 'Benchmark run started')}")


def upload_to_tracker(contract_path: Path) -> None:
    """
    Upload contract bundle to tracker service.

    Creates a bundle containing:
    - agentic_harness package
    - pyproject.toml
    - contracts/{contract_name}/

    Args:
        contract_path: Path to contract directory

    Raises:
        click.ClickException: If upload fails
    """
    UPLOAD_TIMEOUT = 120
    UPLOAD_URL = f"{TRACKER_URL}/upload"

    if not contract_path.exists():
        raise click.ClickException(f"Contract directory not found at {contract_path}")

    project_root = contract_path.parent.parent

    contract_name = contract_path.name
    click.echo(f"Creating bundle for contract '{contract_name}'...")

    # Create temporary directory for bundling
    bundle_path = Path(tempfile.gettempdir()) / "bundle"

    # Clean up if it already exists
    if bundle_path.exists():
        shutil.rmtree(bundle_path)
    bundle_path.mkdir()

    ignore_patterns = shutil.ignore_patterns("__pycache__", "*.pyc")

    try:
        # Copy agentic_harness package
        src_agentic_harness = project_root / "src" / "agentic_harness"
        dst_agentic_harness = bundle_path / "agentic_harness"
        if not src_agentic_harness.exists():
            raise click.ClickException(f"agentic_harness package not found at {src_agentic_harness}")
        shutil.copytree(src_agentic_harness, dst_agentic_harness, ignore=ignore_patterns)

        # Write minimal pyproject.toml for sandbox
        with open(bundle_path / "pyproject.toml", "wb") as f:
            tomli_w.dump(MINIMAL_PYPROJECT, f)

        # Copy contracts/__init__.py
        src_contracts_init = project_root / "contracts" / "__init__.py"
        contracts_dir = bundle_path / "contracts"
        contracts_dir.mkdir(exist_ok=True)
        if not src_contracts_init.exists():
            raise click.ClickException(f"contracts/__init__.py not found at {src_contracts_init}")
        shutil.copy(src_contracts_init, contracts_dir / "__init__.py")

        # Copy contract directory
        dst_contract = contracts_dir / contract_name
        shutil.copytree(contract_path, dst_contract, ignore=ignore_patterns)

        # Zip up the bundle
        click.echo("Zipping bundle...")
        contract_zip_content = zip_directory(bundle_path)
    finally:
        # Clean up temp directory
        shutil.rmtree(bundle_path, ignore_errors=True)

    click.echo(f"Uploading bundle to tracker service at {TRACKER_URL}...")

    files = {
        "contract": (f"{contract_name}.zip", contract_zip_content, "application/zip"),
    }

    try:
        with httpx.Client(timeout=UPLOAD_TIMEOUT) as client:
            response = client.post(UPLOAD_URL, files=files)
    except httpx.RequestError as e:
        raise click.ClickException(f"Failed to request tracker service: {str(e)}")

    if response.status_code != 200:
        raise click.ClickException(f"Upload failed: {response}")

    success_message = response.json().get("message", "Contract uploaded successfully")
    click.echo(success_message)
