"""Tests for isolated SDK artifact verification orchestration."""

from __future__ import annotations

from pathlib import Path

from scripts.sdk.verify_sdk_install import build_commands


def test_verification_covers_both_artifacts_public_api_examples_and_namespace(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    dist.mkdir()
    wheel = dist / "valkyrie_sdk-0.1.0-py3-none-any.whl"
    sdist = dist / "valkyrie_sdk-0.1.0.tar.gz"
    wheel.touch()
    sdist.touch()
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "pyproject.toml").write_text('[project]\nname = "valkyrie"\nversion = "0.1.0"\n', encoding="utf-8")
    commands = build_commands(dist=dist, workspace=workspace, temporary_root=tmp_path / "verify")
    rendered = [" ".join(command.argv) for command in commands]

    assert any(str(wheel) in command and "uv pip install" in command for command in rendered)
    assert any(str(sdist) in command and "uv pip install" in command for command in rendered)
    assert sum("test_sdk.py" in command and "test_models.py" in command for command in rendered) == 2
    assert (
        sum("test_public_api.py::test_public_exports_and_constants_are_stable" in command for command in rendered) == 2
    )
    assert sum("test_public_api.py::test_public_signatures_are_stable" in command for command in rendered) == 2
    assert any("docs/sdk/examples/run_lifecycle.py --help" in command for command in rendered)
    assert any("docs/sdk/examples/manage_run.py --help" in command for command in rendered)
    assert any("uv build --package valkyrie" in command for command in rendered)
    assert any("import valkyrie.cli, valkyrie.sdk" in command for command in rendered)
