"""Validate that Valkyrie SDK artifacts are standalone and publishable."""

from __future__ import annotations

import argparse
import tarfile
import tomllib
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from email.parser import Parser
from pathlib import Path, PurePosixPath
from typing import cast

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

EXPECTED_NAME = "valkyrie-sdk"
EXPECTED_LICENSE = "AGPL-3.0-only"
EXPECTED_LICENSE_FILES = frozenset({"LICENSE"})
FORBIDDEN_REQUIREMENTS = frozenset({"tracker", "valkyrie"})
PACKAGE_PYPROJECT = Path(__file__).resolve().parents[2] / "packages" / "valkyrie-sdk" / "pyproject.toml"
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


@dataclass(frozen=True)
class ExpectedMetadata:
    """Package metadata read from the SDK's pyproject."""

    version: Version
    python: SpecifierSet
    requirements: frozenset[Requirement]


def _expected_metadata() -> ExpectedMetadata:
    with PACKAGE_PYPROJECT.open("rb") as package_file:
        project = tomllib.load(package_file)["project"]

    string_fields = ("version", "requires-python")
    values = {field: project.get(field) for field in string_fields}
    invalid_fields = [field for field, value in values.items() if not isinstance(value, str)]
    if invalid_fields:
        raise ArtifactError(f"SDK pyproject must define string project fields: {', '.join(invalid_fields)}")

    dependencies = project.get("dependencies")
    if not isinstance(dependencies, list) or not all(isinstance(value, str) for value in dependencies):
        raise ArtifactError("SDK pyproject must define project.dependencies as a list of strings")
    if project.get("optional-dependencies"):
        raise ArtifactError("SDK artifact validation does not support project.optional-dependencies")

    requirements = frozenset(Requirement(value) for value in cast(list[str], dependencies))
    forbidden = sorted(
        canonicalize_name(requirement.name)
        for requirement in requirements
        if canonicalize_name(requirement.name) in FORBIDDEN_REQUIREMENTS
    )
    if forbidden:
        raise ArtifactError(f"SDK pyproject declares forbidden runtime dependencies: {forbidden}")

    try:
        version = Version(str(values["version"]))
    except InvalidVersion as exc:
        raise ArtifactError(f"SDK pyproject has an invalid project.version: {values['version']!r}") from exc

    return ExpectedMetadata(
        version=version,
        python=SpecifierSet(str(values["requires-python"])),
        requirements=requirements,
    )


def _validate_metadata(raw_metadata: str) -> ExpectedMetadata:
    expected = _expected_metadata()
    metadata = Parser().parsestr(raw_metadata)
    if canonicalize_name(metadata["Name"] or "") != canonicalize_name(EXPECTED_NAME):
        raise ArtifactError(f"unexpected project name: {metadata['Name']!r}")
    try:
        actual_version = Version(metadata["Version"] or "")
    except InvalidVersion as exc:
        raise ArtifactError(f"invalid project version: {metadata['Version']!r}") from exc
    if actual_version != expected.version:
        raise ArtifactError(f"unexpected project version: {metadata['Version']!r}")
    if metadata["License-Expression"] != EXPECTED_LICENSE:
        raise ArtifactError(f"unexpected license expression: {metadata['License-Expression']!r}")
    if frozenset(metadata.get_all("License-File", [])) != EXPECTED_LICENSE_FILES:
        raise ArtifactError(f"unexpected license files: {metadata.get_all('License-File', [])}")
    if SpecifierSet(metadata["Requires-Python"] or "") != expected.python:
        raise ArtifactError(f"unexpected Python requirement: {metadata['Requires-Python']!r}")

    requirements = frozenset(Requirement(value) for value in metadata.get_all("Requires-Dist", []))
    if requirements != expected.requirements:
        raise ArtifactError(f"unexpected runtime dependencies: {sorted(map(str, requirements))}")
    return expected


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

        expected = _validate_metadata(archive.read(metadata_members[0]).decode())
        normalized_name = canonicalize_name(EXPECTED_NAME).replace("-", "_")
        expected_dist_info = f"{normalized_name}-{expected.version}.dist-info/"
        if dist_info != expected_dist_info:
            raise ArtifactError(f"unexpected wheel metadata directory: {dist_info!r}")


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
