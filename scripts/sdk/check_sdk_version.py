"""Require an SDK version bump when publishable package files change."""

from __future__ import annotations

import argparse
import subprocess
import tomllib
from collections.abc import Sequence
from pathlib import Path

from packaging.version import Version

PACKAGE_PYPROJECT = Path("packages/valkyrie-sdk/pyproject.toml")
PACKAGE_ROOT = PACKAGE_PYPROJECT.parent


class VersionError(RuntimeError):
    """The SDK version does not satisfy the release policy."""


def read_version(contents: bytes) -> Version:
    """Read a PEP 440 version from pyproject bytes."""
    return Version(tomllib.loads(contents.decode())["project"]["version"])


def current_version() -> Version:
    """Read the SDK version from the working tree."""
    return read_version(PACKAGE_PYPROJECT.read_bytes())


def base_version(base_sha: str) -> Version | None:
    """Read the SDK version at a Git revision, if the package existed."""
    result = subprocess.run(
        ["git", "show", f"{base_sha}:{PACKAGE_PYPROJECT.as_posix()}"],
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        return None
    return read_version(result.stdout)


def package_changed(base_sha: str) -> bool:
    """Return whether publishable SDK files changed from the base revision."""
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base_sha}..HEAD", "--", PACKAGE_ROOT.as_posix()],
        check=True,
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip())


def validate_version_change(*, base: Version | None, current: Version, distribution_changed: bool) -> None:
    """Apply initial-release and subsequent semantic version rules."""
    if not distribution_changed:
        return
    if base is None:
        if current != Version("0.1.0"):
            raise VersionError(f"initial SDK version must be 0.1.0, found {current}")
        return
    if current <= base:
        raise VersionError(f"SDK version {current} must be greater than base version {base}")


def main(argv: Sequence[str] | None = None) -> int:
    """Validate the SDK version against a pull request base SHA."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_sha")
    args = parser.parse_args(argv)

    base = base_version(args.base_sha)
    current = current_version()
    changed = package_changed(args.base_sha)
    validate_version_change(base=base, current=current, distribution_changed=changed)
    print(f"SDK version check passed: base={base}, current={current}, package_changed={changed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
