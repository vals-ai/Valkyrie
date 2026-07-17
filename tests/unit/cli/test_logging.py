"""Tests for CLI logging behavior.

Run: uv run pytest tests/unit/cli/test_logging.py
"""

import io
import json
import logging
import os
import shutil
import subprocess
import sys
import threading
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pytest

from valkyrie.cli.logging import configure_cli_logging


@pytest.fixture(autouse=True)
def reset_logging_disable() -> Generator[None, None, None]:
    previous_disable_level = logging.root.manager.disable
    logging.disable(logging.NOTSET)
    yield
    logging.disable(previous_disable_level)


def _emit_info_log() -> str:
    stream = io.StringIO()
    logger = logging.getLogger("httpx")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)

    try:
        logger.info("HTTP Request: GET https://benchmark-tracker.vals.ai/health")
    finally:
        logger.removeHandler(handler)

    return stream.getvalue()


def test_configure_cli_logging_suppresses_logs_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VALKYRIE_CLI_LOGS", raising=False)

    configure_cli_logging()

    assert _emit_info_log() == ""


def test_configure_cli_logging_allows_logs_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALKYRIE_CLI_LOGS", "true")

    configure_cli_logging()

    assert "HTTP Request" in _emit_info_log()


@pytest.mark.parametrize("value", ["1", "TRUE", "yes", "on", "false"])
def test_configure_cli_logging_suppresses_logs_for_non_true_values(value: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VALKYRIE_CLI_LOGS", value)

    configure_cli_logging()

    assert _emit_info_log() == ""


def test_machine_json_subprocess_suppresses_import_time_dotenv_warnings(tmp_path: Path) -> None:
    """A real CLI process must emit exactly one JSON document even when dotenv parsing warns."""
    run_id = uuid4()

    class TrackerHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            path = urlparse(self.path).path
            if path == "/health":
                payload: object = {"status": "healthy"}
            elif path == "/fetch-benchmark":
                payload = {
                    "benchmark_name": "swebench",
                    "benchmark_id": str(run_id),
                    "details": {
                        "status": "IN_PROGRESS",
                        "started_at": "2026-07-09T12:30:00Z",
                        "total_tasks": 1,
                        "finished_tasks": 0,
                        "task_breakdown": {"PENDING": 1},
                        "docent_reading_status": "IDLE",
                    },
                    "s3_bucket_url": "s3://example/run",
                }
            elif path == f"/fetch-benchmark-metadata/{run_id}":
                payload = {
                    "benchmark_id": str(run_id),
                    "benchmark_name": "swebench",
                    "benchmark_arguments": {
                        "contract": {"name": "agent", "model": "openai/gpt-5"},
                        "concurrency": 1,
                        "dataset": "default",
                    },
                }
            else:
                self.send_error(404)
                return

            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *_args: object) -> None:
            del format
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), TrackerHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        project_root = Path(__file__).parents[3]
        tool_root = tmp_path / "tool-root"
        site_packages = tool_root / "lib" / "python3.12" / "site-packages"
        shutil.copytree(project_root / "src" / "valkyrie", site_packages / "valkyrie")
        (tool_root / ".env").write_text("this is not valid dotenv syntax = 'unterminated\n")
        runner = tmp_path / "run_valkyrie.py"
        runner.write_text("from valkyrie.cli.entry import main\n\nmain()\n")

        home = tmp_path / "home"
        config_dir = home / ".config" / "valkyrie"
        config_dir.mkdir(parents=True)
        (config_dir / "valkyrie.yaml").write_text(
            "AWS_ACCESS_KEY_ID: test\n"
            "AWS_SECRET_ACCESS_KEY: test\n"
            "AWS_DEFAULT_REGION: us-east-1\n"
            "S3_BUCKET: test\n"
            "LOG_GROUP: test\n"
            "LOG_RETENTION_POLICY: 1\n"
            "DAYTONA_SECRET_NAME: test\n"
        )

        env = os.environ.copy()
        env["HOME"] = str(home)
        env["PYTHONPATH"] = os.pathsep.join([str(site_packages), *filter(None, [env.get("PYTHONPATH")])])
        env["TRACKER_SERVICE_URL"] = f"http://127.0.0.1:{server.server_port}"
        env.pop("VALKYRIE_CLI_LOGS", None)

        result = subprocess.run(
            [
                sys.executable,
                str(runner),
                "run",
                "fetch",
                str(run_id),
                "--format",
                "json",
            ],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    lines = result.stdout.splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["run_id"] == str(run_id)
