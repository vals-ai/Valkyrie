"""Build zipped agent bundles for upload to S3 (client-side packaging)."""

import os
import shutil
import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Generator

from tracker.exceptions import BundlerError


def _zip_directory_to_file(directory: Path, output_path: Path) -> None:
    """Zip a directory to output_path, skipping venvs, caches, and dotfiles."""
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
def get_agent_zip_stream(agent_name: str | None, agent_path: Path) -> Generator[BinaryIO, None, None]:
    """Yield a readable file handle to a freshly-built zip of the agent at `agent_path`."""
    agent_name = agent_name or agent_path.name

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        bundle_dir = temp_path / agent_name

        if agent_path.is_dir():
            shutil.copytree(agent_path, bundle_dir, dirs_exist_ok=True)
        else:
            bundle_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(agent_path, bundle_dir)

        zip_path = temp_path / f"{agent_name}.zip"
        _zip_directory_to_file(bundle_dir, zip_path)

        with open(zip_path, "rb") as f:
            yield f
