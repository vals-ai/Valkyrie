"""Client-side agent packaging and safe archive extraction."""

import os
import re
import shutil
import stat
import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Generator, cast

import yaml

DEFAULT_MAX_ARCHIVE_BYTES = 1024**3
DEFAULT_MAX_EXPANDED_BYTES = 5 * 1024**3
DEFAULT_MAX_ENTRIES = 100_000


def validate_agent_name(name: str) -> str:
    """Validate names used as archive roots and library keys."""
    if not re.fullmatch(r"[A-Za-z0-9._-]+", name) or name in {".", ".."}:
        raise ValueError("Invalid agent name: use only letters, digits, dots, dashes, or underscores")

    return name


def read_agent_name(agent_path: Path) -> str:
    """Read the library name from contract.yaml or contract.yml."""
    for extension in ("yaml", "yml"):
        contract = agent_path / f"contract.{extension}"
        if contract.is_file():
            data: object = yaml.safe_load(contract.read_text())
            if not isinstance(data, dict) or not isinstance(data.get("name"), str):
                raise ValueError("Agent contract must contain a string name")

            return validate_agent_name(cast(str, data["name"]))

    raise FileNotFoundError(f"No contract.yaml or contract.yml in {agent_path}")


@contextmanager
def get_agent_zip_stream(agent_name: str, agent_path: Path) -> Generator[BinaryIO, None, None]:
    """Bundle a directory, excluding caches and environment files without following symlinks."""
    validate_agent_name(agent_name)
    excluded = {
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
    if agent_path.is_symlink():
        raise ValueError(f"Agent path must not be a symlink: {agent_path}")

    with tempfile.TemporaryFile() as stream:
        with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
            if agent_path.is_dir():
                for root, directories, files in os.walk(agent_path):
                    directories[:] = [directory for directory in directories if directory not in excluded]
                    for entry in [*directories, *files]:
                        path = Path(root) / entry
                        if path.is_symlink():
                            raise ValueError(f"Agent bundles cannot contain symlinks: {path}")
                    for file in files:
                        if any(pattern in file for pattern in excluded):
                            continue
                        path = Path(root) / file
                        if not path.is_file():
                            raise ValueError(f"Agent bundles require regular files: {path}")
                        archive.write(path, f"{agent_name}/{path.relative_to(agent_path).as_posix()}")
            else:
                archive.write(agent_path, f"{agent_name}/{agent_path.name}")
        stream.seek(0)

        yield stream


def extract_agent_archive(
    stream: BinaryIO,
    name: str,
    output_dir: Path,
    *,
    overwrite: bool,
    max_archive_bytes: int = DEFAULT_MAX_ARCHIVE_BYTES,
    max_expanded_bytes: int = DEFAULT_MAX_EXPANDED_BYTES,
    max_entries: int = DEFAULT_MAX_ENTRIES,
) -> Path:
    """Validate every member and CRC in staging before replacing the named target directory."""
    validate_agent_name(name)
    if min(max_archive_bytes, max_expanded_bytes, max_entries) <= 0:
        raise ValueError("Agent archive limits must be positive")
    if stream.seek(0, 2) > max_archive_bytes:
        raise ValueError("Agent archive exceeds max_archive_bytes")
    stream.seek(0)
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / name
    if target.is_symlink() or (target.exists() and (not overwrite or not target.is_dir())):
        raise FileExistsError(f"Target already exists: {target}; use overwrite for an existing directory")

    with tempfile.TemporaryDirectory(dir=output_dir) as temporary:
        staging = Path(temporary)
        with zipfile.ZipFile(stream) as archive:
            members = archive.infolist()
            if len(members) > max_entries:
                raise ValueError("Agent archive exceeds max_entries")
            if sum(member.file_size for member in members) > max_expanded_bytes:
                raise ValueError("Agent archive exceeds max_expanded_bytes")
            paths: set[str] = set()
            for member in members:
                path = member.filename.rstrip("/")
                parts = path.split("/")
                mode = member.external_attr >> 16
                if (
                    member.orig_filename != member.filename
                    or "\\" in path
                    or ":" in path
                    or any(part in {"", ".", ".."} for part in parts)
                    or parts[0] != name
                    or (len(parts) == 1 and not member.is_dir())
                    or stat.S_IFMT(mode) not in {0, stat.S_IFREG, stat.S_IFDIR}
                    or path.casefold() in paths
                ):
                    raise ValueError(f"Unsafe agent archive member: {member.filename!r}")
                paths.add(path.casefold())
            if not any(
                member.filename in {f"{name}/contract.yaml", f"{name}/contract.yml"} and not member.is_dir()
                for member in members
            ):
                raise ValueError("Agent archive is missing its contract")
            expanded_bytes = 0
            for member in members:
                destination = staging / member.filename
                if member.is_dir():
                    destination.mkdir(parents=True, exist_ok=True)
                else:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(member) as source, destination.open("xb") as output:
                        while chunk := source.read(1024 * 1024):
                            expanded_bytes += len(chunk)
                            if expanded_bytes > max_expanded_bytes:
                                raise ValueError("Agent archive exceeds max_expanded_bytes")
                            output.write(chunk)
                    destination.chmod((member.external_attr >> 16) & 0o777 or 0o644)

        if target.is_symlink() or (target.exists() and not overwrite):
            raise FileExistsError(f"Target already exists: {target}")
        backup = None
        if target.exists():
            backup = Path(tempfile.mkdtemp(prefix=f".{name}-backup-", dir=output_dir))
            try:
                target.rename(backup / name)
            except BaseException:
                backup.rmdir()
                raise
        try:
            (staging / name).rename(target)
        except BaseException:
            if backup is not None:
                try:
                    (backup / name).rename(target)
                except OSError as error:
                    raise OSError(f"Agent replacement failed; previous files remain at {backup / name}") from error
                backup.rmdir()
            raise
        if backup is not None:
            shutil.rmtree(backup)

    return target
