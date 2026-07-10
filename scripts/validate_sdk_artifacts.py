"""Validate that Valkyrie SDK artifacts are standalone and publishable."""

from __future__ import annotations

import argparse
import tarfile
import tomllib
import zipfile
from collections.abc import Sequence
from email.parser import Parser
from pathlib import Path, PurePosixPath

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name

EXPECTED_NAME = "valkyrie-sdk"
PACKAGE_PYPROJECT = Path(__file__).parents[1] / "packages" / "valkyrie-sdk" / "pyproject.toml"
EXPECTED_PYTHON = SpecifierSet(">=3.12,<3.13")
EXPECTED_REQUIREMENTS = {
    "httpx": SpecifierSet(">=0.28.1,<1"),
    "pydantic": SpecifierSet(">=2,<3"),
    "pyyaml": SpecifierSet(">=6.0.3,<7"),
}
REQUIRED_WHEEL_MEMBERS = {
    "valkyrie/sdk/__init__.py",
    "valkyrie/sdk/client.py",
    "valkyrie/sdk/py.typed",
}
REQUIRED_SDIST_MEMBERS = {
    ".gitignore",
    "LICENSE",
    "README.md",
    "pyproject.toml",
    "PKG-INFO",
    "src/valkyrie/sdk/__init__.py",
    "src/valkyrie/sdk/py.typed",
}
EXPECTED_GITIGNORE = ".ruff_cache/\n__pycache__/\ndist/\n"


class ArtifactError(RuntimeError):
    """An SDK distribution contains unexpected or incomplete content."""


def _expected_version() -> str:
    with PACKAGE_PYPROJECT.open("rb") as package_file:
        project = tomllib.load(package_file)["project"]
    version = project.get("version")
    if not isinstance(version, str):
        raise ArtifactError("SDK pyproject must define a string project.version")
    return version


def _validate_metadata(raw_metadata: str) -> None:
    metadata = Parser().parsestr(raw_metadata)
    if canonicalize_name(metadata["Name"] or "") != canonicalize_name(EXPECTED_NAME):
        raise ArtifactError(f"unexpected project name: {metadata['Name']!r}")
    if metadata["Version"] != _expected_version():
        raise ArtifactError(f"unexpected project version: {metadata['Version']!r}")
    if metadata["License-Expression"] != "AGPL-3.0-only":
        raise ArtifactError("missing AGPL-3.0-only license expression")
    if "LICENSE" not in metadata.get_all("License-File", []):
        raise ArtifactError("missing LICENSE metadata")
    if SpecifierSet(metadata["Requires-Python"] or "") != EXPECTED_PYTHON:
        raise ArtifactError(f"unexpected Python requirement: {metadata['Requires-Python']!r}")

    requirements = [Requirement(value) for value in metadata.get_all("Requires-Dist", [])]
    actual = {canonicalize_name(requirement.name): requirement.specifier for requirement in requirements}
    if actual != EXPECTED_REQUIREMENTS:
        raise ArtifactError(f"unexpected runtime dependencies: {actual}")


def validate_wheel(path: Path) -> None:
    """Validate wheel members, metadata, license, and typed marker."""
    with zipfile.ZipFile(path) as archive:
        members = {name for name in archive.namelist() if not name.endswith("/")}
        metadata_members = [name for name in members if name.endswith(".dist-info/METADATA")]
        if len(metadata_members) != 1:
            raise ArtifactError(f"expected one METADATA file, found {len(metadata_members)}")
        dist_info = metadata_members[0].removesuffix("METADATA")

        for member in members:
            if not (member.startswith("valkyrie/sdk/") or member.startswith(dist_info)):
                raise ArtifactError(f"forbidden wheel member: {member}")
            if member in {"valkyrie/__init__.py"} or member.startswith("sdk/"):
                raise ArtifactError(f"forbidden wheel member: {member}")

        missing = REQUIRED_WHEEL_MEMBERS - members
        if missing:
            raise ArtifactError(f"missing wheel members: {sorted(missing)}")
        if f"{dist_info}licenses/LICENSE" not in members:
            raise ArtifactError("wheel does not contain its LICENSE file")

        _validate_metadata(archive.read(metadata_members[0]).decode())


def validate_sdist(path: Path) -> None:
    """Validate that an sdist contains only SDK source and packaging files."""
    with tarfile.open(path, "r:gz") as archive:
        files = [member for member in archive.getmembers() if member.isfile()]
        if any(member.issym() or member.islnk() for member in archive.getmembers()):
            raise ArtifactError("sdist must not contain links")

        roots = {PurePosixPath(member.name).parts[0] for member in files}
        if len(roots) != 1:
            raise ArtifactError(f"sdist must have one root directory, found {sorted(roots)}")
        root = next(iter(roots))
        relative_members = {PurePosixPath(member.name).relative_to(root).as_posix() for member in files}

        for member in relative_members:
            if member not in {
                ".gitignore",
                "LICENSE",
                "README.md",
                "pyproject.toml",
                "PKG-INFO",
            } and not member.startswith("src/valkyrie/sdk/"):
                raise ArtifactError(f"forbidden sdist member: {member}")

        missing = REQUIRED_SDIST_MEMBERS - relative_members
        if missing:
            raise ArtifactError(f"missing sdist members: {sorted(missing)}")

        ignore_member = archive.getmember(f"{root}/.gitignore")
        ignore_file = archive.extractfile(ignore_member)
        if ignore_file is None or ignore_file.read().decode() != EXPECTED_GITIGNORE:
            raise ArtifactError("sdist contains an unexpected .gitignore")

        metadata_member = archive.getmember(f"{root}/PKG-INFO")
        metadata_file = archive.extractfile(metadata_member)
        if metadata_file is None:
            raise ArtifactError("sdist PKG-INFO is not readable")
        _validate_metadata(metadata_file.read().decode())


def main(argv: Sequence[str] | None = None) -> int:
    """Validate all artifact paths supplied on the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args(argv)

    for artifact in args.artifacts:
        if artifact.suffix == ".whl":
            validate_wheel(artifact)
        elif artifact.name.endswith(".tar.gz"):
            validate_sdist(artifact)
        else:
            raise ArtifactError(f"unsupported artifact: {artifact}")
        print(f"validated {artifact}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
