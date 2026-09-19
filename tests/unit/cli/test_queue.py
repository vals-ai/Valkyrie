"""Queue CLI behavior through the real SDK and an HTTP mock transport.

Run: uv run pytest tests/unit/cli/test_queue.py
"""

import json
from collections.abc import Callable
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner, Result
from valkyrie.sdk import SchedulerOverviewResponse, ValkyrieClient, ValkyrieConfig

from valkyrie.cli.main import cli

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")
_TASK_ID = UUID("123e4567-e89b-12d3-a456-426614174001")


@pytest.fixture
def overview() -> dict[str, object]:
    """Return a capped snapshot with totals larger than the visible entries."""
    return {
        "observed_at": "2026-09-10T12:02:00+00:00",
        "summary": {"waiting": 12, "building": 2, "in_progress": 3, "evaluating": 1},
        "pools": [{"pool_id": "shared", "waiting": 12}],
        "waiting_entries": [
            {
                "benchmark_uuid": str(_RUN_ID),
                "task_uuid": str(_TASK_ID),
                "benchmark_name": "swebench",
                "external_task_id": "repo__issue-1",
                "pool_id": "shared",
                "position": 2,
                "priority": 1,
                "enqueued_at": "2026-09-10T12:00:00+00:00",
            }
        ],
        "active_entries": [
            {
                "benchmark_uuid": str(_RUN_ID),
                "task_uuid": str(_TASK_ID),
                "benchmark_name": "swebench",
                "external_task_id": "repo__issue-2",
                "status": "EVALUATING",
                "started_at": "2026-09-10T12:00:00+00:00",
            }
        ],
        "waiting_capped": True,
        "active_capped": True,
        "waiting_next_offset": 6,
        "active_next_offset": 4,
    }


@pytest.fixture
def invoke_queue(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Callable[..., Result]:
    """Use a config without AWS credentials and replace only the SDK's HTTP transport."""
    config_path = tmp_path / "queue.yaml"
    config_path.write_text(
        "api_key: test-key\nAWS_DEFAULT_REGION: us-east-1\nS3_BUCKET: test-bucket\n"
        "sandbox_providers:\n  daytona: test-provider\n"
    )
    monkeypatch.setenv("VALKYRIE_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("TRACKER_SERVICE_URL", "https://tracker.test")

    def invoke(handler: Callable[[httpx.Request], httpx.Response], *arguments: str) -> Result:
        def from_config(path: Path, *, base_url: str) -> ValkyrieClient:
            return ValkyrieClient(
                ValkyrieConfig.from_yaml(path),
                base_url=base_url,
                transport=httpx.MockTransport(handler),
            )

        monkeypatch.setattr(ValkyrieClient, "from_config", from_config)
        return CliRunner().invoke(cli, ["queue", "status", *arguments])

    return invoke


def test_status_shows_queue_details_and_capped_totals(
    invoke_queue: Callable[..., Result], overview: dict[str, object]
) -> None:
    """Show pool positions, priorities, wait time, and active states without hiding caps."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=overview)

    result = invoke_queue(handler)

    assert result.exit_code == 0, result.output
    assert "Waiting: 12" in result.output
    assert "shared" in result.output
    assert "P1" in result.output
    assert str(_RUN_ID) in result.output
    assert "repo__issue-1" in result.output
    assert "120" in result.output
    assert "EVALUATING" in result.output
    assert "Showing 1 of 12 waiting tasks" in result.output
    assert "Showing 1 active tasks; entries are capped" in result.output
    assert "Next waiting page: --waiting-offset 6" in result.output
    assert "Next active page: --active-offset 4" in result.output
    assert "Pages are live" in result.output


@pytest.mark.parametrize(("exhausted", "remaining"), [("waiting", "active"), ("active", "waiting")])
def test_status_guides_only_lists_with_more_pages(
    invoke_queue: Callable[..., Result], overview: dict[str, object], exhausted: str, remaining: str
) -> None:
    overview[f"{exhausted}_next_offset"] = None
    overview[f"{exhausted}_capped"] = False

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=overview)

    result = invoke_queue(handler)

    assert result.exit_code == 0, result.output
    assert f"Next {exhausted} page" not in result.output
    assert f"Next {remaining} page: --{remaining}-offset" in result.output


def test_status_json_preserves_entries_and_limits(
    invoke_queue: Callable[..., Result], overview: dict[str, object]
) -> None:
    """Serialize the SDK snapshot while forwarding both limits and API-key authentication."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/scheduler/overview"
        assert dict(request.url.params) == {
            "waiting_limit": "1",
            "active_limit": "2",
            "waiting_offset": "5",
            "active_offset": "3",
        }
        assert request.headers["X-Api-Key"] == "test-key"
        return httpx.Response(200, json=overview)

    result = invoke_queue(
        handler,
        "--format",
        "JSON",
        "--waiting-limit",
        "1",
        "--active-limit",
        "2",
        "--waiting-offset",
        "5",
        "--active-offset",
        "3",
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["summary"]["waiting"] == 12
    assert payload["waiting_entries"][0]["position"] == 2
    assert payload["waiting_entries"][0]["priority"] == 1
    assert payload["active_entries"][0]["status"] == "EVALUATING"
    assert payload["waiting_capped"] is True
    assert payload["active_capped"] is True
    assert payload["waiting_next_offset"] == 6
    assert payload["active_next_offset"] == 4


def test_status_escapes_terminal_controls(invoke_queue: Callable[..., Result], overview: dict[str, object]) -> None:
    """Escape pool and task control characters in tables, but preserve JSON values."""
    snapshot = SchedulerOverviewResponse.model_validate(overview)
    unsafe_pool = "pool\x1b]52;c;dGVzdA==\x07\nforged-pool"
    unsafe_task = "task\x1b]52;c;dGVzdA==\x07\nforged-task"
    snapshot.pools[0].pool_id = unsafe_pool
    snapshot.waiting_entries[0].pool_id = unsafe_pool
    snapshot.waiting_entries[0].external_task_id = unsafe_task
    snapshot.active_entries[0].external_task_id = unsafe_task

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=snapshot.model_dump(mode="json"))

    text_result = invoke_queue(handler)
    json_result = invoke_queue(handler, "--format", "json")

    assert text_result.exit_code == json_result.exit_code == 0
    assert "\x1b" not in text_result.output
    assert "\x07" not in text_result.output
    assert "\nforged" not in text_result.output
    assert text_result.output.count(r"\nforged-pool") == 2
    assert text_result.output.count(r"\nforged-task") == 2
    payload = json.loads(json_result.output)
    assert payload["pools"][0]["pool_id"] == unsafe_pool
    assert payload["waiting_entries"][0]["external_task_id"] == unsafe_task
    assert payload["active_entries"][0]["external_task_id"] == unsafe_task


def test_status_empty_queue(invoke_queue: Callable[..., Result]) -> None:
    """An empty snapshot succeeds without claiming queueing is disabled."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "observed_at": "2026-09-10T12:02:00Z",
                "summary": {},
                "pools": [],
                "waiting_entries": [],
                "active_entries": [],
                "waiting_capped": False,
                "active_capped": False,
                "waiting_next_offset": None,
                "active_next_offset": None,
            },
        )

    result = invoke_queue(handler)

    assert result.exit_code == 0, result.output
    assert "Waiting: 0" in result.output
    assert "No waiting tasks found" in result.output
    assert "No active tasks found" in result.output
    assert "disabled" not in result.output
    assert "Next waiting page" not in result.output
    assert "Next active page" not in result.output


@pytest.mark.parametrize(
    ("flag", "value"),
    [(flag, value) for flag in ("--waiting-limit", "--active-limit") for value in ("0", "201")]
    + [(flag, value) for flag in ("--waiting-offset", "--active-offset") for value in ("-1", "1.5")],
)
def test_status_rejects_invalid_limits(flag: str, value: str) -> None:
    """Reject out-of-range limits before loading credentials or contacting the tracker."""
    result = CliRunner().invoke(cli, ["queue", "status", flag, value])

    assert result.exit_code == 2
    assert flag in result.output


@pytest.mark.parametrize("failure", ["api", "transport", "malformed"])
def test_status_reports_failures(invoke_queue: Callable[..., Result], failure: str) -> None:
    """API, network, and malformed-response failures exit nonzero without partial JSON."""

    def handler(request: httpx.Request) -> httpx.Response:
        if failure == "transport":
            raise httpx.ConnectError("tracker unavailable", request=request)
        if failure == "malformed":
            return httpx.Response(200, json={"summary": {}})
        return httpx.Response(403, json={"detail": "Not authorized"})

    result = invoke_queue(handler, "--format", "json")

    assert result.exit_code == 1
    assert "Error:" in result.output
    assert not result.output.startswith("{")
