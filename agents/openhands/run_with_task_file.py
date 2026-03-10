#!/usr/bin/env python3
"""Run OpenHands with a prompt loaded from a task file."""

from __future__ import annotations

import argparse
import runpy
import shutil
import sys
from pathlib import Path
from typing import Any

import toml

OUTPUT_ROOT = Path("/tmp/openhands_output")
WORKSPACE_ROOT = Path("/workspace")
WORKSPACE_ENV_PATH = WORKSPACE_ROOT / "generated-app" / ".env"
OPENHANDS_LOG_ROOT = Path("/logs/openhands")


def parse_extra_args(raw_args: list[str]) -> dict[str, str]:
    extras: dict[str, str] = {}
    index = 0
    while index < len(raw_args):
        token = raw_args[index]
        if not token.startswith("--"):
            raise SystemExit(f"Unexpected argument: {token}")

        key = token[2:].replace("-", "_")
        if not key:
            raise SystemExit(f"Invalid argument: {token}")

        value = "true"
        if index + 1 < len(raw_args) and not raw_args[index + 1].startswith("--"):
            value = raw_args[index + 1]
            index += 1

        extras[key] = value
        index += 1

    return extras


def parse_args(argv: list[str] | None = None) -> tuple[argparse.Namespace, dict[str, str]]:
    parser = argparse.ArgumentParser()
    parser.add_argument("problem_statement_path")
    parser.add_argument("--model", required=True)
    parser.add_argument("--task-id", required=True)
    args, unknown = parser.parse_known_args(argv)
    return args, parse_extra_args(unknown)


def _coerce_value(value: str) -> Any:
    lowered = value.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true"

    try:
        return int(value)
    except ValueError:
        pass

    try:
        return float(value)
    except ValueError:
        return value


def load_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key] = value.strip().strip('"').strip("'")
    return env


def split_urls(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def copy_path(source: Path, destination: Path) -> None:
    if not source.exists():
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, destination, dirs_exist_ok=True)
    else:
        shutil.copy2(source, destination)


def collect_outputs(task_id: str) -> None:
    if OUTPUT_ROOT.exists():
        shutil.rmtree(OUTPUT_ROOT)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    copy_path(OPENHANDS_LOG_ROOT, OUTPUT_ROOT / "logs" / "openhands")
    copy_path(WORKSPACE_ROOT / "generated-app", OUTPUT_ROOT / "workspace" / "generated-app")
    copy_path(WORKSPACE_ROOT / ".browser_screenshots", OUTPUT_ROOT / "workspace" / ".browser_screenshots")
    copy_path(WORKSPACE_ROOT / ".downloads", OUTPUT_ROOT / "workspace" / ".downloads")
    (OUTPUT_ROOT / "task_id.txt").write_text(task_id + "\n", encoding="utf-8")


def main() -> int:
    args, extra_args = parse_args()

    task_file = Path(args.problem_statement_path)
    if not task_file.exists():
        raise FileNotFoundError(f"Problem statement path not found: {task_file}")

    base_config_path = Path("/bundle/openhands/base_openhands_config.toml")
    if not base_config_path.exists():
        raise FileNotFoundError(f"Missing base config: {base_config_path}")

    prompt_text = task_file.read_text(encoding="utf-8")
    workspace_env = load_env_file(WORKSPACE_ENV_PATH)

    config = toml.loads(base_config_path.read_text(encoding="utf-8"))
    config.setdefault("llm", {})["model"] = args.model

    agent_cfg = config.setdefault("agent", {})
    agent_cfg["enable_browsing"] = True

    core_cfg = config.setdefault("core", {})
    core_cfg["save_trajectory_path"] = f"/logs/openhands/{args.task_id}_trajectory.json"
    core_cfg["workspace_base"] = str(WORKSPACE_ROOT)
    core_cfg["enable_browser"] = True

    if "max_iterations" in extra_args:
        core_cfg["max_iterations"] = _coerce_value(extra_args["max_iterations"])

    max_iterations = core_cfg.get("max_iterations")
    if isinstance(max_iterations, int) and max_iterations <= 0:
        core_cfg["max_iterations"] = 2_147_483_647

    llm_cfg = config.setdefault("llm", {})
    for key in ("temperature", "top_p", "max_tokens"):
        if key in extra_args:
            llm_cfg[key] = _coerce_value(extra_args[key])

    mcp_cfg = config.setdefault("mcp", {})
    mcp_cfg["sse_servers"] = split_urls(workspace_env.get("MCP_SSE_URLS", ""))
    mcp_cfg["shttp_servers"] = split_urls(workspace_env.get("MCP_SHTTP_URLS", ""))

    run_config = Path(f"/tmp/openhands_config_{args.task_id}.toml")
    run_config.write_text(toml.dumps(config), encoding="utf-8")

    from openhands.agenthub.codeact_agent.codeact_agent import CodeActAgent
    from openhands.runtime.plugins import AgentSkillsRequirement

    CodeActAgent.sandbox_plugins = [AgentSkillsRequirement()]

    original_argv = sys.argv[:]
    try:
        sys.argv = [
            "openhands.core.main",
            "--config-file",
            str(run_config),
            "-t",
            prompt_text,
        ]
        try:
            runpy.run_module("openhands.core.main", run_name="__main__")
            return 0
        except SystemExit as exc:
            if isinstance(exc.code, int):
                return exc.code
            return 0 if exc.code is None else 1
    finally:
        collect_outputs(args.task_id)
        sys.argv = original_argv


if __name__ == "__main__":
    raise SystemExit(main())
