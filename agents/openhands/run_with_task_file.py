#!/usr/bin/env python3
"""Run OpenHands with a prompt loaded from a task file."""

from __future__ import annotations

import argparse
import importlib
import runpy
import shutil
import sys
from typing import Any
from pathlib import Path

import toml

OUTPUT_ROOT = Path("/tmp/openhands_output")
WORKSPACE_ROOT = Path("/workspace")
WORKSPACE_ENV_PATH = WORKSPACE_ROOT / "generated-app" / ".env"
OPENHANDS_LOG_ROOT = Path("/logs/openhands")
MAX_ITERATIONS_SENTINEL = 2_147_483_647


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("problem_statement_path")
    parser.add_argument("--model", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--max-iterations", "--max_iterations", dest="max_iterations", type=int)
    parser.add_argument("--temperature", type=float)
    parser.add_argument("--top-p", "--top_p", dest="top_p", type=float)
    parser.add_argument("--max-tokens", "--max_tokens", dest="max_tokens", type=int)
    return parser.parse_args(argv)


def load_mcp_server_urls(path: Path) -> tuple[list[str], list[str]]:
    env: dict[str, str] = {}
    if not path.exists():
        return [], []

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in {"MCP_SSE_URLS", "MCP_SHTTP_URLS"}:
            continue
        env[key] = value.strip().strip('"').strip("'")

    return split_urls(env.get("MCP_SSE_URLS", "")), split_urls(env.get("MCP_SHTTP_URLS", ""))


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


def configure_openhands_runtime() -> None:
    codeact_module = importlib.import_module("openhands.agenthub.codeact_agent.codeact_agent")
    runtime_plugins_module = importlib.import_module("openhands.runtime.plugins")

    codeact_agent: Any = codeact_module.CodeActAgent
    agent_skills_requirement: Any = runtime_plugins_module.AgentSkillsRequirement
    codeact_agent.sandbox_plugins = [agent_skills_requirement()]


def main() -> int:
    args = parse_args()

    task_file = Path(args.problem_statement_path)
    if not task_file.exists():
        raise FileNotFoundError(f"Problem statement path not found: {task_file}")

    base_config_path = Path("/bundle/openhands/base_openhands_config.toml")
    if not base_config_path.exists():
        raise FileNotFoundError(f"Missing base config: {base_config_path}")

    prompt_text = task_file.read_text(encoding="utf-8")
    sse_servers, shttp_servers = load_mcp_server_urls(WORKSPACE_ENV_PATH)

    config = toml.loads(base_config_path.read_text(encoding="utf-8"))
    config.setdefault("llm", {})["model"] = args.model

    agent_cfg = config.setdefault("agent", {})
    agent_cfg["enable_browsing"] = True

    core_cfg = config.setdefault("core", {})
    core_cfg["save_trajectory_path"] = f"/logs/openhands/{args.task_id}_trajectory.json"
    core_cfg["workspace_base"] = str(WORKSPACE_ROOT)
    core_cfg["enable_browser"] = True

    if args.max_iterations is not None:
        core_cfg["max_iterations"] = args.max_iterations

    max_iterations = core_cfg.get("max_iterations")
    if isinstance(max_iterations, int) and max_iterations <= 0:
        core_cfg["max_iterations"] = MAX_ITERATIONS_SENTINEL

    llm_cfg = config.setdefault("llm", {})
    for key in ("temperature", "top_p", "max_tokens"):
        value = getattr(args, key)
        if value is not None:
            llm_cfg[key] = value

    mcp_cfg = config.setdefault("mcp", {})
    mcp_cfg["sse_servers"] = sse_servers
    mcp_cfg["shttp_servers"] = shttp_servers

    run_config = Path(f"/tmp/openhands_config_{args.task_id}.toml")
    run_config.write_text(toml.dumps(config), encoding="utf-8")

    configure_openhands_runtime()

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
