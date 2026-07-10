"""Tests for SDK wheel and source-distribution validation."""

from __future__ import annotations

import io
import tarfile
import zipfile
from pathlib import Path

import pytest

from scripts import validate_sdk_artifacts
from scripts.validate_sdk_artifacts import ArtifactError, validate_sdist, validate_wheel

METADATA = """Metadata-Version: 2.4
Name: valkyrie-sdk
Version: 0.1.0
License-Expression: AGPL-3.0-only
License-File: LICENSE
Requires-Python: >=3.12
Requires-Dist: httpx<1,>=0.28.1
Requires-Dist: pydantic<3,>=2
Requires-Dist: pyyaml<7,>=6.0.3
"""


def write_wheel(path: Path, *, extra_members: dict[str, str] | None = None, metadata: str = METADATA) -> None:
    members = {
        "valkyrie/sdk/__init__.py": "",
        "valkyrie/sdk/client.py": "",
        "valkyrie/sdk/py.typed": "",
        "valkyrie_sdk-0.1.0.dist-info/METADATA": metadata,
        "valkyrie_sdk-0.1.0.dist-info/WHEEL": "Wheel-Version: 1.0\n",
        "valkyrie_sdk-0.1.0.dist-info/licenses/LICENSE": "AGPL",
        "valkyrie_sdk-0.1.0.dist-info/RECORD": "",
    }
    members.update(extra_members or {})
    with zipfile.ZipFile(path, "w") as archive:
        for name, contents in members.items():
            archive.writestr(name, contents)


def write_sdist(path: Path, *, extra_members: dict[str, str] | None = None) -> None:
    members = {
        "valkyrie_sdk-0.1.0/.gitignore": ".ruff_cache/\n__pycache__/\ndist/\n",
        "valkyrie_sdk-0.1.0/LICENSE": "AGPL",
        "valkyrie_sdk-0.1.0/README.md": "# Valkyrie SDK",
        "valkyrie_sdk-0.1.0/pyproject.toml": "[project]\nname='valkyrie-sdk'\n",
        "valkyrie_sdk-0.1.0/PKG-INFO": METADATA,
        "valkyrie_sdk-0.1.0/src/valkyrie/sdk/__init__.py": "",
        "valkyrie_sdk-0.1.0/src/valkyrie/sdk/py.typed": "",
    }
    members.update(extra_members or {})
    with tarfile.open(path, "w:gz") as archive:
        for name, contents in members.items():
            encoded = contents.encode()
            info = tarfile.TarInfo(name)
            info.size = len(encoded)
            archive.addfile(info, io.BytesIO(encoded))


def test_valid_artifacts_are_accepted(tmp_path: Path) -> None:
    wheel = tmp_path / "valkyrie_sdk-0.1.0-py3-none-any.whl"
    sdist = tmp_path / "valkyrie_sdk-0.1.0.tar.gz"
    write_wheel(wheel)
    write_sdist(sdist)

    validate_wheel(wheel)
    validate_sdist(sdist)


def test_expected_version_comes_from_the_sdk_pyproject(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[project]\nname = "valkyrie-sdk"\nversion = "0.1.1"\n', encoding="utf-8")
    monkeypatch.setattr(validate_sdk_artifacts, "PACKAGE_PYPROJECT", pyproject)
    wheel = tmp_path / "valkyrie_sdk-0.1.1-py3-none-any.whl"
    write_wheel(wheel, metadata=METADATA.replace("Version: 0.1.0", "Version: 0.1.1"))

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


def test_sdist_rejects_unrelated_repository_files(tmp_path: Path) -> None:
    sdist = tmp_path / "bad.tar.gz"
    write_sdist(sdist, extra_members={"valkyrie_sdk-0.1.0/services/tracker/app.py": ""})

    with pytest.raises(ArtifactError, match="forbidden sdist member"):
        validate_sdist(sdist)
