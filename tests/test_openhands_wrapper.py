from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "agents" / "openhands" / "run_with_task_file.py"
SPEC = importlib.util.spec_from_file_location("openhands_run_with_task_file", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_parse_args_supports_explicit_runtime_overrides() -> None:
    args = MODULE.parse_args(
        [
            "task.txt",
            "--model",
            "anthropic/test-model",
            "--task-id",
            "task_1",
            "--max_iterations",
            "0",
            "--temperature",
            "0.7",
            "--top_p",
            "0.9",
            "--max_tokens",
            "2048",
        ]
    )

    assert args.problem_statement_path == "task.txt"
    assert args.model == "anthropic/test-model"
    assert args.task_id == "task_1"
    assert args.max_iterations == 0
    assert args.temperature == pytest.approx(0.7)
    assert args.top_p == pytest.approx(0.9)
    assert args.max_tokens == 2048


def test_parse_args_rejects_unknown_options() -> None:
    with pytest.raises(SystemExit):
        MODULE.parse_args(
            [
                "task.txt",
                "--model",
                "anthropic/test-model",
                "--task-id",
                "task_1",
                "--unexpected",
                "value",
            ]
        )


def test_load_mcp_server_urls_only_reads_supported_keys(tmp_path: Path) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "MCP_SSE_URLS=http://sse-a,http://sse-b",
                "MCP_SHTTP_URLS=http://shttp-a",
                "UNRELATED_KEY=ignored",
            ]
        ),
        encoding="utf-8",
    )

    sse_servers, shttp_servers = MODULE.load_mcp_server_urls(env_path)

    assert sse_servers == ["http://sse-a", "http://sse-b"]
    assert shttp_servers == ["http://shttp-a"]


def test_configure_runtime_environment_sets_default_playwright_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PLAYWRIGHT_BROWSERS_PATH", raising=False)

    MODULE.configure_runtime_environment()

    assert os.environ["PLAYWRIGHT_BROWSERS_PATH"] == MODULE.DEFAULT_PLAYWRIGHT_BROWSERS_PATH
