#!/usr/bin/env python3
"""Run OpenHands with a prompt loaded from a task file."""

from __future__ import annotations

import argparse
import runpy
import sys
from pathlib import Path

import toml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("problem_statement_path")
    parser.add_argument("--model", required=True)
    parser.add_argument("--task-id", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    task_file = Path(args.problem_statement_path)
    if not task_file.exists():
        raise FileNotFoundError(f"Problem statement path not found: {task_file}")

    base_config_path = Path("/bundle/openhands/base_openhands_config.toml")
    if not base_config_path.exists():
        raise FileNotFoundError(f"Missing base config: {base_config_path}")

    config = toml.loads(base_config_path.read_text(encoding="utf-8"))
    config.setdefault("llm", {})["model"] = args.model
    config.setdefault("core", {})["save_trajectory_path"] = f"/logs/openhands/{args.task_id}_trajectory.json"

    run_config = Path(f"/tmp/openhands_config_{args.task_id}.toml")
    run_config.write_text(toml.dumps(config), encoding="utf-8")

    # LocalRuntime loads sandbox plugins from CodeActAgent.sandbox_plugins directly.
    # Force-disable Jupyter plugin to avoid kernel startup dependency in benchmark sandboxes.
    from openhands.agenthub.codeact_agent.codeact_agent import CodeActAgent
    from openhands.runtime.plugins import AgentSkillsRequirement

    CodeActAgent.sandbox_plugins = [AgentSkillsRequirement()]

    original_argv = sys.argv[:]
    try:
        sys.argv = [
            "openhands.core.main",
            "--config-file",
            str(run_config),
            "-f",
            str(task_file),
        ]
        try:
            runpy.run_module("openhands.core.main", run_name="__main__")
            return 0
        except SystemExit as exc:
            if isinstance(exc.code, int):
                return exc.code
            return 0 if exc.code is None else 1
    finally:
        sys.argv = original_argv


if __name__ == "__main__":
    raise SystemExit(main())
