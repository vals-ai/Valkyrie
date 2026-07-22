"""Tests for SDK wheel and source-distribution validation."""

from __future__ import annotations

import io
import tarfile
import tomllib
import zipfile
from pathlib import Path

import pytest

from scripts.sdk import validate_sdk_artifacts
from scripts.sdk.validate_sdk_artifacts import ArtifactError, validate_sdist, validate_wheel

ROOT = Path(__file__).parents[3]
PACKAGE_ROOT = ROOT / "packages" / "valkyrie-sdk"
with (PACKAGE_ROOT / "pyproject.toml").open("rb") as package_file:
    SDK_VERSION = str(tomllib.load(package_file)["project"]["version"])

METADATA = f"""Metadata-Version: 2.4
Name: valkyrie-sdk
Version: {SDK_VERSION}
License-Expression: AGPL-3.0-only
License-File: LICENSE
Requires-Python: >=3.12
Requires-Dist: httpx<1,>=0.28.1
Requires-Dist: pydantic<3,>=2
Requires-Dist: pyyaml<7,>=6.0.3
"""


def write_wheel(
    path: Path,
    *,
    extra_members: dict[str, str] | None = None,
    metadata: str = METADATA,
    dist_info_version: str = SDK_VERSION,
) -> None:
    dist_info = f"valkyrie_sdk-{dist_info_version}.dist-info"
    members = {
        "valkyrie/sdk/__init__.py": "",
        "valkyrie/sdk/client.py": "",
        "valkyrie/sdk/py.typed": "",
        f"{dist_info}/METADATA": metadata,
        f"{dist_info}/WHEEL": "Wheel-Version: 1.0\n",
        f"{dist_info}/licenses/LICENSE": "AGPL",
        f"{dist_info}/RECORD": "",
    }
    members.update(extra_members or {})
    with zipfile.ZipFile(path, "w") as archive:
        for name, contents in members.items():
            archive.writestr(name, contents)


def write_sdist(path: Path, *, extra_members: dict[str, str] | None = None) -> None:
    root = f"valkyrie_sdk-{SDK_VERSION}"
    members = {
        f"{root}/.gitignore": ".ruff_cache/\n__pycache__/\ndist/\n",
        f"{root}/LICENSE": "AGPL",
        f"{root}/README.md": "# Valkyrie SDK",
        f"{root}/pyproject.toml": "[project]\nname='valkyrie-sdk'\n",
        f"{root}/PKG-INFO": METADATA,
        f"{root}/src/valkyrie/sdk/__init__.py": "",
        f"{root}/src/valkyrie/sdk/py.typed": "",
    }
    members.update(extra_members or {})
    with tarfile.open(path, "w:gz") as archive:
        for name, contents in members.items():
            encoded = contents.encode()
            info = tarfile.TarInfo(name)
            info.size = len(encoded)
            archive.addfile(info, io.BytesIO(encoded))


def test_sdk_package_configuration_matches_release_boundaries() -> None:
    with (ROOT / "pyproject.toml").open("rb") as root_file:
        root_project = tomllib.load(root_file)
    with (PACKAGE_ROOT / "pyproject.toml").open("rb") as sdk_file:
        sdk_project = tomllib.load(sdk_file)

    assert root_project["project"]["requires-python"] == ">=3.12,<3.13"
    assert sdk_project["project"]["requires-python"] == ">=3.12"
    assert root_project["tool"]["basedpyright"]["pythonVersion"] == "3.12"
    assert sdk_project["tool"]["basedpyright"]["pythonVersion"] == "3.12"
    assert sdk_project["build-system"]["requires"] == ["hatchling==1.27.0"]
    assert (PACKAGE_ROOT / ".gitignore").read_text(encoding="utf-8") == ".ruff_cache/\n__pycache__/\ndist/\n"


def test_valid_artifacts_are_accepted(tmp_path: Path) -> None:
    wheel = tmp_path / f"valkyrie_sdk-{SDK_VERSION}-py3-none-any.whl"
    sdist = tmp_path / f"valkyrie_sdk-{SDK_VERSION}.tar.gz"
    write_wheel(wheel)
    write_sdist(sdist)

    validate_wheel(wheel)
    validate_sdist(sdist)


def test_version_python_and_dependencies_come_from_sdk_pyproject(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """[project]
name = "valkyrie-sdk"
version = "0.1.1"
requires-python = ">=3.11"
license = "AGPL-3.0-only"
license-files = ["LICENSE"]
dependencies = ["httpx>=0.27,<2"]
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(validate_sdk_artifacts, "PACKAGE_PYPROJECT", pyproject)
    wheel = tmp_path / "valkyrie_sdk-0.1.1-py3-none-any.whl"
    metadata = """Metadata-Version: 2.4
Name: valkyrie-sdk
Version: 0.1.1
License-Expression: AGPL-3.0-only
License-File: LICENSE
Requires-Python: >=3.11
Requires-Dist: httpx<2,>=0.27
"""
    write_wheel(wheel, metadata=metadata, dist_info_version="0.1.1")

    validate_wheel(wheel)


@pytest.mark.parametrize(
    "member",
    ["valkyrie/cli/main.py", "tracker/types.py", "valkyrie/__init__.py", "sdk/client.py"],
)
def test_wheel_rejects_forbidden_members(tmp_path: Path, member: str) -> None:
    wheel = tmp_path / "bad.whl"
    write_wheel(wheel, extra_members={member: ""})

    with pytest.raises(ArtifactError, match="forbidden wheel member"):
        validate_wheel(wheel)


def test_wheel_rejects_tracker_dependency(tmp_path: Path) -> None:
    wheel = tmp_path / "bad.whl"
    write_wheel(wheel, metadata=f"{METADATA}Requires-Dist: tracker\n")

    with pytest.raises(ArtifactError, match="unexpected runtime dependencies"):
        validate_wheel(wheel)


def test_wheel_rejects_forbidden_dependency_declared_by_pyproject(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nversion = "0.1.0"\nrequires-python = ">=3.12"\ndependencies = ["tracker"]\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(validate_sdk_artifacts, "PACKAGE_PYPROJECT", pyproject)
    wheel = tmp_path / "bad.whl"
    metadata = "\n".join(line for line in METADATA.splitlines() if not line.startswith("Requires-Dist:"))
    write_wheel(wheel, metadata=f"{metadata}\nRequires-Dist: tracker\n")

    with pytest.raises(ArtifactError, match="forbidden runtime dependencies"):
        validate_wheel(wheel)


def test_wheel_rejects_unsupported_optional_dependencies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        '[project]\nversion = "0.1.0"\nrequires-python = ">=3.12"\ndependencies = []\n'
        '[project.optional-dependencies]\ncli = ["click>=8"]\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(validate_sdk_artifacts, "PACKAGE_PYPROJECT", pyproject)
    wheel = tmp_path / "bad.whl"
    write_wheel(wheel)

    with pytest.raises(ArtifactError, match="does not support project.optional-dependencies"):
        validate_wheel(wheel)


def test_wheel_rejects_mismatched_dist_info_version(tmp_path: Path) -> None:
    wheel = tmp_path / "bad.whl"
    write_wheel(wheel, dist_info_version="999.0.0")

    with pytest.raises(ArtifactError, match="unexpected wheel metadata directory"):
        validate_wheel(wheel)


def test_sdist_rejects_unrelated_repository_files(tmp_path: Path) -> None:
    sdist = tmp_path / "bad.tar.gz"
    write_sdist(sdist, extra_members={f"valkyrie_sdk-{SDK_VERSION}/services/tracker/app.py": ""})

    with pytest.raises(ArtifactError, match="forbidden sdist member"):
        validate_sdist(sdist)
