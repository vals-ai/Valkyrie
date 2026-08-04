"""Relevance selector for the required-ci aggregate gate.

Computes, from the exact ``pull_request.base.sha``/``pull_request.head.sha`` diff, which
subsystem validations a PR revision must run. Emits one boolean per leaf job to
``$GITHUB_OUTPUT`` plus the diff identities the aggregate and reviewers rely on.

A leaf job runs only when its flag is ``true``. The aggregate treats a skipped leaf as a
pass only when the flag it publishes here is ``false``; a leaf that was required but did
not succeed fails the gate. Any failure in this selector fails the gate.
"""

from __future__ import annotations

import fnmatch
import os
import subprocess
import sys
from pathlib import Path

# Subsystem path rules. A rule ending in ``/**`` matches that directory recursively;
# any other rule matches the exact path. Mirrors the path filters of the leaf workflows
# these jobs supersede (tracker/executor/infra/sdk/lockfile), verified against the
# real workflows in .github/workflows on the base branch.
ROOT_PKG = [
    "src/valkyrie/**",
    "pyproject.toml",
    "uv.lock",
    "Makefile",
    "tests/**",
]
TRACKER = ["services/tracker/**"]
EXECUTOR = [
    ".dockerignore",
    ".github/workflows/executor-build.yaml",
    ".github/workflows/deploy.yaml",
    "infra/executor_stack.py",
    "infra/tracker_stack.py",
    "services/executor_artifact/**",
    "services/executor_host/**",
    "services/tracker/**",
    "tests/unit/executor_host/**",
]
INFRA = [
    "infra/**",
    ".github/workflows/deploy.yaml",
    ".github/workflows/executor-build.yaml",
    ".github/workflows/infra-ci.yaml",
]
SDK = [
    "packages/valkyrie-sdk/**",
    "src/valkyrie/__init__.py",
    "pyproject.toml",
    "uv.lock",
    "tests/unit/sdk/**",
    "tests/contract/**",
    "tests/fixtures/sdk_api/**",
    "services/tracker/main.py",
    "services/tracker/pyproject.toml",
    "services/tracker/uv.lock",
    "services/tracker/src/tracker/**",
    "docs/sdk/examples/**",
    "scripts/sdk/**",
    ".github/workflows/sdk-package.yml",
    ".github/workflows/publish-sdk.yml",
]
LOCKFILE_ROOT = ["uv.lock", "pyproject.toml"]
LOCKFILE_TRACKER = ["services/tracker/uv.lock", "services/tracker/pyproject.toml"]
LOCKFILE_INFRA = ["infra/uv.lock", "infra/pyproject.toml"]

# Changing the gate itself, its selector, its aggregate, or the required-context manifest
# forces full validation and — via the trusted maintenance-classification gate — cannot be
# self-approved by required-ci alone.
SELF_PROTECTED = [
    ".github/workflows/required-ci.yaml",
    ".github/scripts/required_ci_select.py",
    ".github/scripts/required_ci_aggregate.py",
    ".github/required-contexts.json",
]


def _matches(path: str, rule: str) -> bool:
    if rule.endswith("/**"):
        prefix = rule[:-2]
        return path == rule[:-3] or path.startswith(prefix)
    return fnmatch.fnmatch(path, rule)


def _any(paths: list[str], rules: list[str]) -> bool:
    return any(_matches(path, rule) for path in paths for rule in rules)


def select(paths: list[str], *, base_ref: str, is_fork: bool) -> dict[str, str]:
    """Pure selection: map a changed-path set to per-leaf required flags."""
    force_all = _any(paths, SELF_PROTECTED)
    is_prod = base_ref == "prod"

    root_pkg = force_all or _any(paths, ROOT_PKG)
    tracker = force_all or _any(paths, TRACKER)
    executor = force_all or _any(paths, EXECUTOR)
    infra = force_all or _any(paths, INFRA)
    sdk = force_all or _any(paths, SDK)
    lockfile_root = force_all or _any(paths, LOCKFILE_ROOT)
    lockfile_tracker = force_all or _any(paths, LOCKFILE_TRACKER)
    lockfile_infra = force_all or _any(paths, LOCKFILE_INFRA)

    any_code = root_pkg or tracker or executor or infra or sdk

    return {
        "base_ref": base_ref,
        "is_fork": _b(is_fork),
        "is_prod": _b(is_prod),
        "force_all": _b(force_all),
        # Lint/typecheck run for any code change (docs-only PRs skip them).
        "run_lint": _b(any_code),
        "run_typecheck": _b(any_code),
        # Root package == CLI validation (cli-tests + cli-tool-smoke-test).
        "run_cli": _b(root_pkg),
        "run_cli_smoke": _b(root_pkg),
        "run_tracker_unit": _b(tracker),
        # Live tracker validation only for same-repo tracker PRs; fork PRs are blocked.
        "run_tracker_live": _b(tracker and not is_fork),
        "run_tracker_live_fork_blocked": _b(tracker and is_fork),
        "run_executor": _b(executor),
        "run_infra": _b(infra),
        "run_sdk": _b(sdk),
        "run_lockfile_root": _b(lockfile_root),
        "run_lockfile_tracker": _b(lockfile_tracker),
        "run_lockfile_infra": _b(lockfile_infra),
        # CBS pin validation is a prod-only requirement.
        "run_cbs": _b(is_prod),
    }


def _changed_paths(base_sha: str, head_sha: str) -> list[str]:
    output = subprocess.run(
        ["git", "diff", "--name-only", f"{base_sha}", f"{head_sha}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [line.strip() for line in output.splitlines() if line.strip()]


def _emit(outputs: dict[str, str]) -> None:
    output_path = os.environ["GITHUB_OUTPUT"]
    with Path(output_path).open("a", encoding="utf-8") as handle:
        for key, value in outputs.items():
            handle.write(f"{key}={value}\n")


def _b(value: bool) -> str:
    return "true" if value else "false"


def main() -> int:
    base_sha = os.environ["BASE_SHA"]
    head_sha = os.environ["HEAD_SHA"]
    base_ref = os.environ.get("BASE_REF", "")
    is_fork = os.environ.get("IS_FORK", "false") == "true"

    paths = _changed_paths(base_sha, head_sha)

    outputs = {"base_sha": base_sha, "head_sha": head_sha}
    outputs.update(select(paths, base_ref=base_ref, is_fork=is_fork))
    _emit(outputs)

    print(f"base_sha={base_sha} head_sha={head_sha} base_ref={base_ref} is_fork={is_fork}")
    print(f"changed files ({len(paths)}):")
    for path in paths:
        print(f"  {path}")
    print("selection:")
    for key, value in outputs.items():
        if key.startswith("run_") or key == "force_all":
            print(f"  {key}={value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
