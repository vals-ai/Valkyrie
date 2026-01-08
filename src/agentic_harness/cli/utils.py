"""Utility functions for the CLI."""

import os
import tempfile
import zipfile
from pathlib import Path

import click
import httpx

from agentic_harness.cli.config import TRACKER_URL


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


def upload_to_tracker(agent_path: Path, contract_path: Path) -> None:
    """
    Upload agent and contract to tracker service.

    Args:
        agent_path: Path to agent submodule directory
        contract_path: Path to contract file

    Raises:
        click.ClickException: If upload fails
    """
    UPLOAD_TIMEOUT = 120  # seconds

    click.echo("Zipping agent directory...")
    agent_zip_content = zip_directory(agent_path)

    click.echo("Reading contract file...")
    with open(contract_path, "rb") as f:
        contract_content = f.read()

    click.echo(f"Uploading to tracker service at {TRACKER_URL}...")

    upload_url = f"{TRACKER_URL}/upload"
    files = {
        "agent": (f"{agent_path.name}.zip", agent_zip_content, "application/zip"),
        "contract": (contract_path.name, contract_content, "text/x-python"),
    }

    try:
        with httpx.Client(timeout=UPLOAD_TIMEOUT) as client:
            response = client.post(upload_url, files=files)
    except httpx.RequestError as e:
        raise click.ClickException(f"Failed to request tracker service: {str(e)}")

    if response.status_code == 200:
        success_message = response.json().get("message", "Agent and contract uploaded successfully")
        click.echo(success_message)
    else:
        error_detail = response.json().get("detail", "Unknown error")
        raise click.ClickException(f"Upload failed: {error_detail}")
