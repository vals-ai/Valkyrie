"""Classify one Git change for normal or maintenance deployment."""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast

from classify_executor_template_change import (
    ExecutorHostTemplateEffect,
    classify_executor_host_template_change,
)

_EMPTY_TREE_SHA = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
_MIGRATION_DIRECTORY = "services/tracker/src/tracker/database/migrations/versions/"
_EXECUTOR_STACK_FILES = {
    ".dockerignore",
    ".github/workflows/deploy.yaml",
    ".github/workflows/maintenance-classification.yaml",
    "infra/Makefile",
    "infra/app.py",
    "infra/cdk.json",
    "infra/classify_repository_change.py",
    "infra/constants.py",
    "infra/executor_release/main.py",
    "infra/executor_stack.py",
    "infra/shared.py",
    "infra/stage.py",
    "infra/stage_config.py",
    "services/tracker/src/executor_protocol.py",
    "services/tracker/src/tracker/executor/maintenance_control.py",
    "services/tracker/src/tracker/executor/release_control.py",
    "services/tracker/src/tracker/executor/release_entrypoint.py",
}
_EXECUTOR_STACK_DIRECTORIES = ("infra/executor_release/", "services/executor_host/")
_EXECUTOR_SHARED_FILES = {
    "infra/app.py",
    "infra/constants.py",
    "infra/shared.py",
    "infra/stage.py",
    "infra/stage_config.py",
}
_EXECUTOR_RELEASE_FILES = {
    "services/executor_artifact/build.py",
    "services/tracker/pyproject.toml",
    "services/tracker/src/executor_protocol.py",
    "services/tracker/src/tracker/__init__.py",
    "services/tracker/src/tracker/_lambda.py",
    "services/tracker/src/tracker/auth.py",
    "services/tracker/src/tracker/config.py",
    "services/tracker/src/tracker/docent_analysis.py",
    "services/tracker/src/tracker/exceptions.py",
    "services/tracker/src/tracker/executor/dispatch_control.py",
    "services/tracker/src/tracker/executor/entrypoint.py",
    "services/tracker/src/tracker/executor/execution_authority.py",
    "services/tracker/src/tracker/executor/release_control.py",
    "services/tracker/src/tracker/notifications.py",
    "services/tracker/src/tracker/outbound_security.py",
    "services/tracker/src/tracker/sandbox.py",
    "services/tracker/src/tracker/types.py",
}
_EXECUTOR_RELEASE_DIRECTORIES = (
    "services/executor_artifact/",
    "services/tracker/src/tracker/agent/",
    "services/tracker/src/tracker/aws/",
    "services/tracker/src/tracker/database/",
    "services/tracker/src/tracker/logging/",
    "services/tracker/src/tracker/middleware/",
    "services/tracker/src/tracker/observability/",
    "services/tracker/src/tracker/utils/",
)


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    operation: str


@dataclass(frozen=True)
class Classification:
    classification: str
    base_sha: str
    head_sha: str
    executor_stack_deploy_required: bool
    executor_host_redeploy_required: bool
    executor_release_required: bool
    core_maintenance_required: bool
    database_maintenance_required: bool
    changed_migrations: list[str]
    reasons: list[str]
    findings: list[Finding]


def _git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=repository,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"git {' '.join(arguments)} failed")
    return result.stdout


def _changed_entries(repository: Path, base_sha: str, head_sha: str) -> list[tuple[str, list[str]]]:
    comparison_base = _EMPTY_TREE_SHA if base_sha and set(base_sha) == {"0"} else base_sha
    output = _git(repository, "diff", "--name-status", "--find-renames", comparison_base, head_sha)
    entries: list[tuple[str, list[str]]] = []
    for line in output.splitlines():
        fields = line.split("\t")
        if len(fields) < 2:
            raise RuntimeError(f"Invalid git diff entry: {line!r}")
        entries.append((fields[0][0], fields[1:]))
    return entries


def _call_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name):
        return f"{call.func.value.id}.{call.func.attr}"
    return None


def _is_constant_boolean_server_default(value: ast.expr) -> bool:
    return (
        isinstance(value, ast.Call)
        and _call_name(value) in {"sa.false", "sa.true"}
        and not value.args
        and not value.keywords
    )


def _is_safe_add_column(call: ast.Call) -> bool:
    if _call_name(call) != "op.add_column" or len(call.args) < 2:
        return False
    column = call.args[1]
    if not isinstance(column, ast.Call) or _call_name(column) != "sa.Column" or len(column.args) != 2:
        return False
    if len(column.keywords) == 1:
        nullable = column.keywords[0]
        return (
            nullable.arg == "nullable"
            and isinstance(nullable.value, ast.Constant)
            and isinstance(nullable.value.value, bool)
            and nullable.value.value
        )
    if len(call.args) != 2 or call.keywords or len(column.keywords) != 2:
        return False
    keywords = {keyword.arg: keyword.value for keyword in column.keywords}
    if set(keywords) != {"nullable", "server_default"}:
        return False
    column_type = column.args[1]
    nullable = keywords["nullable"]
    return (
        isinstance(column_type, ast.Call)
        and _call_name(column_type) == "sa.Boolean"
        and not column_type.args
        and not column_type.keywords
        and isinstance(nullable, ast.Constant)
        and nullable.value is False
        and _is_constant_boolean_server_default(keywords["server_default"])
    )


def _is_non_unique_index(call: ast.Call) -> bool:
    if _call_name(call) != "op.create_index":
        return False
    return len(call.keywords) == 1 and any(
        keyword.arg == "unique"
        and isinstance(keyword.value, ast.Constant)
        and isinstance(keyword.value.value, bool)
        and not keyword.value.value
        for keyword in call.keywords
    )


def _is_nullable_alter_column(call: ast.Call) -> bool:
    if _call_name(call) != "op.alter_column" or len(call.args) != 2:
        return False
    allowed_keywords = {"nullable", "schema"}
    for keyword in call.keywords:
        if keyword.arg is None:
            return False
        if keyword.arg.startswith("existing_"):
            continue
        if keyword.arg not in allowed_keywords:
            return False
    return any(
        keyword.arg == "nullable"
        and isinstance(keyword.value, ast.Constant)
        and isinstance(keyword.value.value, bool)
        and keyword.value.value
        for keyword in call.keywords
    )


def _migration_findings(source: str, path: str) -> list[Finding]:
    try:
        module = ast.parse(source, filename=path)
    except SyntaxError as error:
        raise RuntimeError(f"Unable to parse migration {path}: {error}") from error

    upgrades = [node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "upgrade"]
    if len(upgrades) != 1:
        raise RuntimeError(f"Migration {path} must define exactly one upgrade function")

    findings: list[Finding] = []
    for statement in module.body:
        if isinstance(statement, (ast.Import, ast.ImportFrom, ast.FunctionDef)):
            continue
        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant):
            continue
        if isinstance(statement, (ast.Assign, ast.AnnAssign)) and not any(
            isinstance(node, ast.Call) for node in ast.walk(statement)
        ):
            continue
        findings.append(
            Finding(
                path=path,
                line=statement.lineno,
                operation=f"module.{type(statement).__name__}",
            )
        )

    for statement in upgrades[0].body:
        if isinstance(statement, ast.Pass):
            continue
        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Constant):
            continue
        if isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call):
            call = statement.value
            operation = _call_name(call)
            if (
                operation == "op.create_table"
                or _is_safe_add_column(call)
                or _is_non_unique_index(call)
                or _is_nullable_alter_column(call)
            ):
                continue
            findings.append(Finding(path=path, line=statement.lineno, operation=operation or "dynamic call"))
            continue
        findings.append(Finding(path=path, line=statement.lineno, operation=type(statement).__name__))
    return findings


def _is_executor_stack_path(path: str) -> bool:
    return path in _EXECUTOR_STACK_FILES or path.startswith(_EXECUTOR_STACK_DIRECTORIES)


def _is_executor_release_path(path: str) -> bool:
    if path.startswith(_MIGRATION_DIRECTORY):
        return False
    return path in _EXECUTOR_RELEASE_FILES or path.startswith(_EXECUTOR_RELEASE_DIRECTORIES)


def classify_repository_change(
    repository: Path,
    *,
    base_sha: str,
    head_sha: str,
    executor_effect: ExecutorHostTemplateEffect,
) -> Classification:
    changed_migrations: list[str] = []
    findings: list[Finding] = []
    reasons: set[str] = set()
    executor_stack_deploy_required = False
    executor_release_required = False
    core_maintenance_required = False
    database_maintenance_required = False

    for status, paths in _changed_entries(repository, base_sha, head_sha):
        if any(_is_executor_stack_path(path) for path in paths):
            executor_stack_deploy_required = True
            reasons.add("executor-core-change")
        if any(path in _EXECUTOR_SHARED_FILES for path in paths):
            core_maintenance_required = True
        if any(_is_executor_release_path(path) for path in paths):
            executor_release_required = True
            reasons.add("executor-release-change")
        migration_paths = [path for path in paths if path.startswith(_MIGRATION_DIRECTORY)]
        if not migration_paths:
            continue
        if status != "A" or len(migration_paths) != 1:
            raise RuntimeError(f"Applied migration history is immutable: {status} {' -> '.join(paths)}")
        path = migration_paths[0]
        changed_migrations.append(path)
        source = _git(repository, "show", f"{head_sha}:{path}")
        migration_findings = _migration_findings(source, path)
        if migration_findings:
            database_maintenance_required = True
            reasons.add("unsafe-migration")
            findings.extend(migration_findings)

    if executor_effect.redeploy_required:
        executor_stack_deploy_required = True
        reasons.update(executor_effect.reasons)
    maintenance_required = executor_effect.redeploy_required or database_maintenance_required
    classification = "maintenance-required" if maintenance_required else "safe"
    return Classification(
        classification=classification,
        base_sha=base_sha,
        head_sha=head_sha,
        executor_stack_deploy_required=executor_stack_deploy_required,
        executor_host_redeploy_required=executor_effect.redeploy_required,
        executor_release_required=executor_release_required,
        core_maintenance_required=core_maintenance_required,
        database_maintenance_required=database_maintenance_required,
        changed_migrations=sorted(changed_migrations),
        reasons=sorted(reasons),
        findings=findings,
    )


def combine_executor_effects(*effects: ExecutorHostTemplateEffect) -> ExecutorHostTemplateEffect:
    return ExecutorHostTemplateEffect(
        redeploy_required=any(effect.redeploy_required for effect in effects),
        reasons=tuple(sorted({reason for effect in effects for reason in effect.reasons})),
    )


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--repository-root", type=Path, default=Path("."))
    parser.add_argument("--executor-base-template", type=Path, required=True)
    parser.add_argument("--executor-head-template", type=Path, required=True)
    parser.add_argument("--expected-stack-id", required=True)
    parser.add_argument("--secondary-executor-base-template", type=Path)
    parser.add_argument("--secondary-executor-head-template", type=Path)
    parser.add_argument("--secondary-expected-stack-id")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    arguments = _parse_arguments()
    try:
        base_template = json.loads(arguments.executor_base_template.read_text(encoding="utf-8"))
        head_template = json.loads(arguments.executor_head_template.read_text(encoding="utf-8"))
        if not isinstance(base_template, dict) or not isinstance(head_template, dict):
            raise ValueError("WorkerStack templates must be JSON objects")
        executor_effect = classify_executor_host_template_change(
            cast(dict[str, object], base_template),
            cast(dict[str, object], head_template),
            expected_stack_id=arguments.expected_stack_id,
        )
        secondary_inputs = (
            arguments.secondary_executor_base_template,
            arguments.secondary_executor_head_template,
            arguments.secondary_expected_stack_id,
        )
        if any(secondary_inputs) and not all(secondary_inputs):
            raise ValueError("Secondary WorkerStack classification requires both templates and the stack ID")
        if all(secondary_inputs):
            secondary_base_template = json.loads(arguments.secondary_executor_base_template.read_text(encoding="utf-8"))
            secondary_head_template = json.loads(arguments.secondary_executor_head_template.read_text(encoding="utf-8"))
            if not isinstance(secondary_base_template, dict) or not isinstance(secondary_head_template, dict):
                raise ValueError("Secondary WorkerStack templates must be JSON objects")
            secondary_effect = classify_executor_host_template_change(
                cast(dict[str, object], secondary_base_template),
                cast(dict[str, object], secondary_head_template),
                expected_stack_id=arguments.secondary_expected_stack_id,
            )
            executor_effect = combine_executor_effects(executor_effect, secondary_effect)
        payload: dict[str, object] = asdict(
            classify_repository_change(
                arguments.repository_root.resolve(),
                base_sha=arguments.base_sha,
                head_sha=arguments.head_sha,
                executor_effect=executor_effect,
            )
        )
    except Exception as error:
        payload = {
            "classification": "infrastructure-error",
            "base_sha": arguments.base_sha,
            "head_sha": arguments.head_sha,
            "error": str(error),
        }
        rendered = json.dumps(payload, indent=2, sort_keys=True)
        arguments.output.write_text(rendered + "\n", encoding="utf-8")
        print(rendered)
        raise SystemExit(2) from error

    rendered = json.dumps(payload, indent=2, sort_keys=True)
    arguments.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
