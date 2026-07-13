"""Tests for SDK version and release-manifest tooling."""

from __future__ import annotations

import hashlib
import io
import tarfile
import zipfile
from email.message import Message
from pathlib import Path
from typing import Never
from urllib.error import HTTPError, URLError

import pytest
from packaging.version import Version

from scripts.sdk.check_sdk_version import VersionError, validate_version_change
from scripts.sdk.prepare_sdk_release import (
    ReleaseError,
    build_manifest,
    ensure_index_version_available,
    write_checksums,
    write_output,
    write_summary,
)


def test_initial_package_must_start_at_0_1_0() -> None:
    validate_version_change(base=None, current=Version("0.1.0"), distribution_changed=True)

    with pytest.raises(VersionError, match="initial SDK version must be 0.1.0"):
        validate_version_change(base=None, current=Version("0.2.0"), distribution_changed=True)


def test_distribution_changes_require_a_higher_version() -> None:
    with pytest.raises(VersionError, match="must be greater"):
        validate_version_change(base=Version("0.1.0"), current=Version("0.1.0"), distribution_changed=True)

    validate_version_change(base=Version("0.1.0"), current=Version("0.1.1"), distribution_changed=True)
    validate_version_change(base=Version("0.1.0"), current=Version("0.1.0"), distribution_changed=False)


def write_artifacts(dist: Path) -> tuple[Path, Path]:
    wheel = dist / "valkyrie_sdk-0.1.0-py3-none-any.whl"
    sdist = dist / "valkyrie_sdk-0.1.0.tar.gz"
    metadata = "Metadata-Version: 2.4\nName: valkyrie-sdk\nVersion: 0.1.0\n"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("valkyrie_sdk-0.1.0.dist-info/METADATA", metadata)
    with tarfile.open(sdist, "w:gz") as archive:
        contents = b"source"
        info = tarfile.TarInfo("valkyrie_sdk-0.1.0/PKG-INFO")
        info.size = len(contents)
        archive.addfile(info, io.BytesIO(contents))
    return wheel, sdist


def test_release_manifest_binds_two_files_to_name_version_and_hash(tmp_path: Path) -> None:
    wheel, sdist = write_artifacts(tmp_path)

    manifest = build_manifest(tmp_path)

    assert manifest.name == "valkyrie-sdk"
    assert manifest.version == "0.1.0"
    assert manifest.files == {
        wheel.name: hashlib.sha256(wheel.read_bytes()).hexdigest(),
        sdist.name: hashlib.sha256(sdist.read_bytes()).hexdigest(),
    }


def test_checksums_are_relative_to_manifest_directory(tmp_path: Path) -> None:
    packages = tmp_path / "packages"
    packages.mkdir()
    write_artifacts(packages)

    manifest = build_manifest(packages)
    checksum_path = tmp_path / "SHA256SUMS"
    write_checksums(manifest, packages, checksum_path)

    lines = checksum_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert all("  packages/valkyrie_sdk-0.1.0" in line for line in lines)


def test_github_metadata_uses_real_lines(tmp_path: Path) -> None:
    packages = tmp_path / "packages"
    packages.mkdir()
    write_artifacts(packages)
    manifest = build_manifest(packages)
    output = tmp_path / "GITHUB_OUTPUT"
    summary = tmp_path / "GITHUB_STEP_SUMMARY"

    write_output(output, manifest, "pypi")
    write_summary(summary, manifest, "pypi")

    assert output.read_text(encoding="utf-8").splitlines()[:3] == [
        "name=valkyrie-sdk",
        "version=0.1.0",
        "target=pypi",
    ]
    assert summary.read_text(encoding="utf-8").splitlines()[0] == "## Valkyrie SDK release"


def test_index_404_means_version_is_available() -> None:
    def not_found(_request: object) -> Never:
        raise HTTPError("https://pypi.test", 404, "Not Found", Message(), None)

    ensure_index_version_available("pypi", "valkyrie-sdk", "0.1.0", opener=not_found)


@pytest.mark.parametrize(
    "error",
    [
        HTTPError("https://pypi.test", 500, "Server Error", Message(), None),
        URLError("connection failed"),
    ],
)
def test_index_network_errors_fail_with_release_context(error: URLError) -> None:
    def unavailable(_request: object) -> Never:
        raise error

    with pytest.raises(ReleaseError, match="could not check pypi for valkyrie-sdk 0.1.0"):
        ensure_index_version_available("pypi", "valkyrie-sdk", "0.1.0", opener=unavailable)


def test_existing_index_version_fails_loudly() -> None:
    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    with pytest.raises(ReleaseError, match="already exists"):
        ensure_index_version_available("testpypi", "valkyrie-sdk", "0.1.0", opener=lambda _request: Response())
