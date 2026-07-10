"""Static security and structure tests for SDK GitHub Actions workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import yaml

ROOT = Path(__file__).parents[3]
WORKFLOWS = ROOT / ".github" / "workflows"

CHECKOUT = "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0"
SETUP_PYTHON = "actions/setup-python@ece7cb06caefa5fff74198d8649806c4678c61a1"
SETUP_UV = "astral-sh/setup-uv@11f9893b081a58869d3b5fccaea48c9e9e46f990"
UPLOAD = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
DOWNLOAD = "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
PUBLISH = "pypa/gh-action-pypi-publish@cef221092ed1bacb1cc03d23a2d87d1d172e277b"


def load_workflow(name: str) -> dict[str, Any]:
    with (WORKFLOWS / name).open(encoding="utf-8") as workflow_file:
        return cast(dict[str, Any], yaml.safe_load(workflow_file))


def action_uses(job: dict[str, Any]) -> list[str]:
    return [step["uses"] for step in job["steps"] if "uses" in step]


def action_step(job: dict[str, Any], action: str) -> dict[str, Any]:
    return next(step for step in job["steps"] if step.get("uses") == action)


def test_sdk_package_workflow_uses_pinned_read_only_build() -> None:
    workflow = load_workflow("sdk-package.yml")
    package = workflow["jobs"]["package"]

    assert package["runs-on"] == "ubuntu-24.04"
    assert package["permissions"] == {"contents": "read"}
    uses = action_uses(package)
    assert CHECKOUT in uses
    assert SETUP_PYTHON in uses
    assert SETUP_UV in uses
    assert UPLOAD in uses
    assert all("@v" not in action for action in uses)
    assert action_step(package, CHECKOUT)["with"]["persist-credentials"] is False
    assert action_step(package, UPLOAD)["with"]["if-no-files-found"] == "error"


def test_publish_workflow_is_oidc_only_and_prod_guarded() -> None:
    workflow = load_workflow("publish-sdk.yml")
    build = workflow["jobs"]["build"]
    publish = workflow["jobs"]["publish"]

    assert workflow["concurrency"] == {
        "group": "valkyrie-sdk-publish",
        "cancel-in-progress": False,
        "queue": "max",
    }
    assert publish["runs-on"] == "ubuntu-24.04"
    assert publish["permissions"] == {"id-token": "write"}
    assert publish["environment"]["name"] == "${{ needs.build.outputs.target }}"
    assert "github.ref == 'refs/heads/prod'" in publish["if"]
    build_uses = action_uses(build)
    assert CHECKOUT in build_uses
    assert SETUP_PYTHON in build_uses
    assert SETUP_UV in build_uses
    assert UPLOAD in build_uses
    assert all("@v" not in action and "@release/" not in action for action in build_uses)
    assert action_step(build, CHECKOUT)["with"]["persist-credentials"] is False
    assert action_step(build, UPLOAD)["with"]["if-no-files-found"] == "error"
    assert "enable-cache" not in action_step(build, SETUP_UV).get("with", {})
    uses = action_uses(publish)
    assert DOWNLOAD in uses
    assert uses.count(PUBLISH) == 2
    assert all("@v" not in action and "@release/" not in action for action in uses)
    target_step = next(step for step in build["steps"] if step.get("id") == "target")
    assert "refs/heads/dev" in target_step["run"]
    assert "refs/heads/prod" in target_step["run"]


def test_publish_workflow_has_no_long_lived_or_skip_credentials() -> None:
    contents = (WORKFLOWS / "publish-sdk.yml").read_text(encoding="utf-8").lower()

    for line in contents.splitlines():
        assert not line.lstrip().startswith(("password:", "user:", "token:"))
    assert "skip-existing" not in contents
    assert "repository-url: ${{" not in contents


def test_dependabot_reviews_action_pin_updates_on_dev() -> None:
    dependabot = yaml.safe_load((ROOT / ".github" / "dependabot.yml").read_text(encoding="utf-8"))
    update = dependabot["updates"][0]

    assert update["package-ecosystem"] == "github-actions"
    assert update["target-branch"] == "dev"


def test_workflows_use_the_root_dev_dependency_group() -> None:
    for name in ("sdk-package.yml", "publish-sdk.yml"):
        contents = (WORKFLOWS / name).read_text(encoding="utf-8")
        assert "uv sync --group dev --group test" not in contents
        assert "uv sync --locked --group dev" in contents


def test_workflows_clean_install_the_exact_built_artifacts() -> None:
    for name in ("sdk-package.yml", "publish-sdk.yml"):
        contents = (WORKFLOWS / name).read_text(encoding="utf-8")
        assert "scripts/verify_sdk_install.py --dist release/packages" in contents


def test_sdk_ci_artifact_name_uses_the_member_version() -> None:
    workflow = load_workflow("sdk-package.yml")
    package = workflow["jobs"]["package"]
    upload = next(step for step in package["steps"] if step.get("uses") == UPLOAD)

    assert upload["with"]["name"] == "valkyrie-sdk-${{ steps.version.outputs.version }}"
