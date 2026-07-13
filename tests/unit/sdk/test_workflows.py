"""Security and structure checks for SDK workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).parents[3]
WORKFLOWS = ROOT / ".github" / "workflows"
APPROVED_ACTIONS = {
    "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
    "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1",
    "astral-sh/setup-uv@11f9893b081a58869d3b5fccaea48c9e9e46f990",
    "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
    "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
    "pypa/gh-action-pypi-publish@cef221092ed1bacb1cc03d23a2d87d1d172e277b",
}


def load(name: str) -> dict[str, Any]:
    with (WORKFLOWS / name).open(encoding="utf-8") as workflow_file:
        return cast(dict[str, Any], yaml.safe_load(workflow_file))


def actions(job: dict[str, Any]) -> list[dict[str, Any]]:
    return [step for step in job["steps"] if "uses" in step]


def action(job: dict[str, Any], prefix: str) -> dict[str, Any]:
    return next(step for step in actions(job) if step["uses"].startswith(prefix))


def test_external_actions_are_immutable_and_fail_closed() -> None:
    package_workflow = load("sdk-package.yml")
    package = package_workflow["jobs"]["package"]
    publish_workflow = load("publish-sdk.yml")
    build = publish_workflow["jobs"]["build"]

    jobs = [*package_workflow["jobs"].values(), *publish_workflow["jobs"].values()]
    assert all(job["runs-on"] == "ubuntu-24.04" for job in jobs)
    assert all(step["uses"] in APPROVED_ACTIONS for job in jobs for step in actions(job))
    for job in (package, build):
        assert job["permissions"] == {"contents": "read"}
        assert action(job, "actions/checkout@")["with"]["persist-credentials"] is False
        assert action(job, "actions/upload-artifact@")["with"]["if-no-files-found"] == "error"
    assert "enable-cache" not in action(build, "astral-sh/setup-uv@").get("with", {})


def test_publish_is_oidc_only_and_ref_guarded() -> None:
    workflow = load("publish-sdk.yml")
    build = workflow["jobs"]["build"]
    publish = workflow["jobs"]["publish"]
    target_script = next(step["run"] for step in build["steps"] if step.get("id") == "target")
    contents = (WORKFLOWS / "publish-sdk.yml").read_text(encoding="utf-8").lower()

    assert workflow["concurrency"] == {"group": "valkyrie-sdk-publish", "cancel-in-progress": False, "queue": "max"}
    assert publish["permissions"] == {"id-token": "write"}
    assert build["outputs"]["environment"] == "${{ steps.target.outputs.environment }}"
    assert publish["environment"]["name"] == "${{ needs.build.outputs.environment }}"
    assert 'environment="pypi-test"' in target_script
    assert "github.ref == 'refs/heads/prod'" in publish["if"]
    assert "refs/heads/dev" in target_script and "refs/heads/prod" in target_script
    assert not any(line.lstrip().startswith(("password:", "user:", "token:")) for line in contents.splitlines())
    assert "skip-existing" not in contents
    assert "repository-url: ${{" not in contents
    assert "repository-url: https://test.pypi.org/legacy/" in contents
    publish_actions = [step["uses"] for step in actions(publish)]
    assert sum(item.startswith("actions/download-artifact@") for item in publish_actions) == 1
    assert sum(item.startswith("pypa/gh-action-pypi-publish@") for item in publish_actions) == 2


def test_workflows_verify_the_exact_artifacts() -> None:
    expected_scripts = {
        "sdk-package.yml": ("check_sdk_version.py", "validate_sdk_artifacts.py", "verify_sdk_install.py"),
        "publish-sdk.yml": ("prepare_sdk_release.py", "validate_sdk_artifacts.py", "verify_sdk_install.py"),
    }
    for name, scripts in expected_scripts.items():
        contents = (WORKFLOWS / name).read_text(encoding="utf-8")
        assert "uv sync --locked --group dev" in contents
        for script in scripts:
            assert f"scripts/sdk/{script}" in contents

    package = load("sdk-package.yml")["jobs"]["package"]
    upload = action(package, "actions/upload-artifact@")
    assert upload["with"]["name"] == "valkyrie-sdk-${{ steps.version.outputs.version }}"


def test_sdk_ci_checks_newer_python_versions() -> None:
    compatibility = load("sdk-package.yml")["jobs"]["compatibility"]
    assert compatibility["needs"] == "package"
    assert compatibility["strategy"]["matrix"]["python-version"] == ["3.13", "3.14"]
    install = next(step["run"] for step in compatibility["steps"] if step["name"] == "Install and import SDK")
    assert "--prerelease=disallow" in install
