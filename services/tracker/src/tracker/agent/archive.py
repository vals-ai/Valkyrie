"""Validate uploaded agent archives before publishing them to the shared library."""

import stat
import zipfile
from typing import BinaryIO

import yaml

from tracker import config
from tracker.agent.schemas import AgentContract


class ArchiveLimitError(ValueError):
    """An agent archive exceeds an operator-configured resource limit."""


def validate_agent_archive(stream: BinaryIO, name: str) -> None:
    """Check metadata first, then paths, actual expanded bytes, CRCs, and the YAML contract."""
    with zipfile.ZipFile(stream) as archive:
        members = archive.infolist()
        if len(members) > config.AGENT_ARCHIVE_MAX_ENTRIES:
            raise ArchiveLimitError("Agent archive exceeds AGENT_ARCHIVE_MAX_ENTRIES")
        if sum(member.file_size for member in members) > config.AGENT_ARCHIVE_MAX_EXPANDED_BYTES:
            raise ArchiveLimitError("Agent archive exceeds AGENT_ARCHIVE_MAX_EXPANDED_BYTES")

        paths: dict[str, bool] = {}
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
                or member.flag_bits & 1
                or path.casefold() in paths
            ):
                raise ValueError(f"Unsafe agent archive member: {member.filename!r}")
            paths[path.casefold()] = member.is_dir()
        for path in paths:
            parts = path.split("/")
            if any(paths.get("/".join(parts[:index])) is False for index in range(1, len(parts))):
                raise ValueError("Agent archive contains conflicting file and directory paths")

        contract_members = [
            member
            for member in members
            if member.filename in {f"{name}/contract.yaml", f"{name}/contract.yml"} and not member.is_dir()
        ]
        if not contract_members:
            raise ValueError("Agent archive must contain contract.yaml or contract.yml under its named root")

        expanded_bytes = 0
        for member in members:
            with archive.open(member) as source:
                while chunk := source.read(1024 * 1024):
                    expanded_bytes += len(chunk)
                    if expanded_bytes > config.AGENT_ARCHIVE_MAX_EXPANDED_BYTES:
                        raise ArchiveLimitError("Agent archive exceeds AGENT_ARCHIVE_MAX_EXPANDED_BYTES")
        for member in contract_members:
            AgentContract.model_validate(yaml.safe_load(archive.read(member)))

    stream.seek(0)
