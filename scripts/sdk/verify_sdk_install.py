"""Clean-install and exercise built Valkyrie SDK distributions."""

from __future__ import annotations

import argparse
import subprocess
import tempfile
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


class InstallVerificationError(RuntimeError):
    """The distribution set cannot be verified unambiguously."""


@dataclass(frozen=True)
class Command:
    """One verification command and its working directory."""

    argv: tuple[str, ...]
    cwd: Path | None = None


def _one_match(dist: Path, pattern: str) -> Path:
    matches = sorted(dist.glob(pattern))
    if len(matches) != 1:
        raise InstallVerificationError(f"expected one {pattern} in {dist}, found {len(matches)}")
    return matches[0].resolve()


def _project_version(pyproject: Path) -> str:
    with pyproject.open("rb") as project_file:
        project = tomllib.load(project_file)["project"]
    version = project.get("version")
    if not isinstance(version, str):
        raise InstallVerificationError(f"{pyproject} must define a string project.version")
    return version


def _artifact_commands(*, artifact: Path, label: str, workspace: Path, temporary_root: Path) -> list[Command]:
    environment = temporary_root / label
    python = environment / "bin" / "python"
    pytest = environment / "bin" / "pytest"
    sdk_tests = workspace / "tests" / "unit" / "sdk"
    smoke = (
        "import importlib.util; "
        "from valkyrie.sdk import ValkyrieClient; "
        "from valkyrie.sdk.resources import RunsResource; "
        "assert ValkyrieClient and RunsResource; "
        "assert importlib.util.find_spec('tracker') is None; "
        "assert importlib.util.find_spec('valkyrie.cli') is None"
    )
    public_tests = (
        str(sdk_tests / "test_public_api.py") + "::test_public_exports_and_constants_are_stable",
        str(sdk_tests / "test_public_api.py") + "::test_public_signatures_are_stable",
    )
    commands = [
        Command(("uv", "venv", str(environment), "--python", "3.12"), workspace),
        Command(("uv", "pip", "install", "--python", str(python), "--prerelease=disallow", str(artifact)), workspace),
        Command(
            (
                "uv",
                "pip",
                "install",
                "--python",
                str(python),
                "pytest>=9,<10",
                "pytest-asyncio>=1.3,<2",
            ),
            workspace,
        ),
        Command((str(python), "-c", smoke), workspace),
        Command(
            (
                str(pytest),
                f"--confcutdir={sdk_tests}",
                str(sdk_tests / "test_sdk.py"),
                str(sdk_tests / "test_models.py"),
                *public_tests,
                "-q",
            ),
            workspace,
        ),
    ]
    if label == "sdk-wheel":
        commands.extend(
            [
                Command(
                    (str(python), str(workspace / "docs" / "sdk" / "examples" / "run_lifecycle.py"), "--help"),
                    workspace,
                ),
                Command(
                    (str(python), str(workspace / "docs" / "sdk" / "examples" / "manage_run.py"), "--help"), workspace
                ),
            ]
        )
    return commands


def build_commands(*, dist: Path, workspace: Path, temporary_root: Path) -> list[Command]:
    """Return commands that verify both SDK artifacts and the shared namespace."""
    dist = dist.resolve()
    workspace = workspace.resolve()
    wheel = _one_match(dist, "*.whl")
    sdist = _one_match(dist, "*.tar.gz")
    commands = [
        *_artifact_commands(artifact=wheel, label="sdk-wheel", workspace=workspace, temporary_root=temporary_root),
        *_artifact_commands(artifact=sdist, label="sdk-sdist", workspace=workspace, temporary_root=temporary_root),
    ]

    root_dist = temporary_root / "root-dist"
    root_version = _project_version(workspace / "pyproject.toml")
    root_wheel = root_dist / f"valkyrie-{root_version}-py3-none-any.whl"
    combo = temporary_root / "sdk-with-root"
    combo_python = combo / "bin" / "python"
    commands.extend(
        [
            Command(("uv", "build", "--package", "valkyrie", "--no-sources", "--out-dir", str(root_dist)), workspace),
            Command(("uv", "venv", str(combo), "--python", "3.12"), workspace),
            Command(
                ("uv", "pip", "install", "--python", str(combo_python), "--prerelease=disallow", str(wheel)), workspace
            ),
            Command(("uv", "pip", "install", "--python", str(combo_python), "--no-deps", str(root_wheel)), workspace),
            Command((str(combo_python), "-c", "import valkyrie.cli, valkyrie.sdk"), workspace),
        ]
    )
    return commands


def main(argv: Sequence[str] | None = None) -> int:
    """Run isolated artifact installation and namespace compatibility checks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory(prefix="valkyrie-sdk-verify-") as temporary_directory:
        commands = build_commands(
            dist=args.dist,
            workspace=args.workspace,
            temporary_root=Path(temporary_directory),
        )
        for command in commands:
            subprocess.run(command.argv, cwd=command.cwd, check=True)
    print("isolated SDK artifact verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
