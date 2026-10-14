"""Tests for run start.

Run: uv run pytest tests/unit/cli/run/test_start.py -v

Covers counted starts, the contract sent to Tracker, local agents, and failure reporting.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner, Result
from valkyrie.sdk.models import BenchmarkStatus, TaskStatus

from valkyrie.cli.run.start import start

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import make_fetch_response, run_stream, serve_run

_FIRST_RUN_ID = UUID("00000000-0000-0000-0000-000000000001")
_SECOND_RUN_ID = UUID("00000000-0000-0000-0000-000000000002")
_THIRD_RUN_ID = UUID("00000000-0000-0000-0000-000000000003")
_UNKNOWN_OUTCOME = "outcome may be unknown"
_LOCAL_CONTRACT = """\
name: local-agent
install_cmd: echo install
run_cmd: run {problem_statement_path} --variant {variant}
kwargs:
  variant: {type: str, required: true, choices: [max, min]}
"""


def _started(run_id: UUID) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "benchmark_name": "swebench",
            "agent_name": "remote-agent",
            "benchmark_id": str(run_id),
            "concurrency": 5,
            "started_at": datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc).isoformat(),
            "task_count": 2,
            "cloudwatch_url": f"https://cloudwatch.example/{run_id}",
            "s3_bucket_url": f"s3://runs/{run_id}",
        },
    )


def _start_bodies(tracker: MockTracker) -> list[dict[str, object]]:
    return [json.loads(request.content) for request in tracker.requested("POST", "/start-benchmark")]


def _invoke(*arguments: str, agent: str = "remote-agent") -> Result:
    return CliRunner().invoke(start, ["--agent", agent, "--benchmark", "swebench", *arguments])


def test_single_start_sends_the_agent_by_name_and_prints_next_steps(tracker: MockTracker) -> None:
    """Tracker resolves the published contract, so the request carries only name, model, kwargs and secrets."""
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID))

    result = _invoke("--model", "anthropic/claude-opus-5", "-k", "variant", "max", "-s", "API_KEY", "custom-secret")

    assert result.exit_code == 0, result.output
    (body,) = _start_bodies(tracker)
    assert body["concurrency"] == 5
    contract = body["contract"]
    assert (contract["name"], contract["model"], contract["run_cmd"]) == ("remote-agent", "anthropic/claude-opus-5", "")
    assert (contract["kwargs"], contract["secrets"]) == ({"variant": "max"}, {"API_KEY": "custom-secret"})
    assert f"valkyrie run fetch {_FIRST_RUN_ID} --connect" in result.output
    assert f"valkyrie run outputs {_FIRST_RUN_ID} --output-dir ." in result.output
    assert "requested runs successfully started" not in result.output


def test_connected_start_stays_connected_while_tasks_are_discovered(tracker: MockTracker) -> None:
    discovered = make_fetch_response(
        _FIRST_RUN_ID, total_tasks=2, finished_tasks=0, task_breakdown={TaskStatus.PENDING: 1, TaskStatus.BUILDING: 1}
    )
    completed = make_fetch_response(_FIRST_RUN_ID, status=BenchmarkStatus.FINISHED, total_tasks=2, finished_tasks=2)
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID))
    serve_run(
        tracker,
        make_fetch_response(_FIRST_RUN_ID, total_tasks=0, finished_tasks=0, task_breakdown={}),
        stream=run_stream(discovered, completed),
    )

    result = _invoke("-n", "1", "--connect")

    assert result.exit_code == 0, result.output
    assert "0/0 (0.0%)" in result.output
    assert "Pending: 1" in result.output
    assert "2/2 (100.0%)" in result.output
    assert "✓ Run completed!" in result.output


def test_counted_start_reuses_one_contract_and_reports_every_run(tracker: MockTracker) -> None:
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID), _started(_SECOND_RUN_ID))

    result = _invoke("-k", "temperature", "1", "--label", "stable", "--count", "2")

    assert result.exit_code == 0, result.output
    first, second = _start_bodies(tracker)
    assert first == second
    assert first["label"] == "stable"
    assert first["contract"]["kwargs"] == {"temperature": "1"}

    details, summary = result.output.split("2 / 2 requested runs successfully started.", maxsplit=1)
    assert str(_FIRST_RUN_ID) in details
    assert str(_SECOND_RUN_ID) in details
    assert f"Track progress: valkyrie run status --ids {_FIRST_RUN_ID},{_SECOND_RUN_ID}" in summary


def test_counted_local_start_publishes_once_without_overwriting(tracker: MockTracker, tmp_path: Path) -> None:
    (tmp_path / "contract.yaml").write_text(_LOCAL_CONTRACT)
    tracker.reply("PUT", "/agents/local-agent", httpx.Response(200, json={"name": "local-agent"}))
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID), _started(_SECOND_RUN_ID))

    result = _invoke("-k", "variant", "max", "--count", "2", agent=str(tmp_path))

    assert result.exit_code == 0, result.output
    (upload,) = tracker.requested("PUT", "/agents/local-agent")
    assert upload.url.params["overwrite"] == "false"
    assert [body["contract"]["name"] for body in _start_bodies(tracker)] == ["local-agent", "local-agent"]


@pytest.mark.parametrize(
    ("contract", "arguments", "upload", "exit_code", "message"),
    [
        (
            _LOCAL_CONTRACT,
            ["-k", "variant", "max"],
            httpx.Response(409, json={"detail": "exists"}),
            2,
            "Use --agent local-agent",
        ),
        (_LOCAL_CONTRACT, ["-k", "variant", "huge"], None, 1, "'variant' has an invalid value"),
        ("name: local-agent\n", [], None, 1, "Invalid contract"),
    ],
    ids=["already-published", "invalid-kwarg", "invalid-contract"],
)
def test_local_start_stops_before_starting_a_run(
    tracker: MockTracker,
    tmp_path: Path,
    contract: str,
    arguments: list[str],
    upload: httpx.Response | None,
    exit_code: int,
    message: str,
) -> None:
    """A published agent is never replaced, and an invalid contract or kwarg is never uploaded."""
    (tmp_path / "contract.yaml").write_text(contract)
    if upload is not None:
        tracker.reply("PUT", "/agents/local-agent", upload)

    result = _invoke(*arguments, agent=str(tmp_path))

    assert result.exit_code == exit_code
    assert message in result.output
    assert len(tracker.requested("PUT", "/agents/local-agent")) == (upload is not None)
    assert _start_bodies(tracker) == []


@pytest.mark.parametrize(
    "arguments",
    [
        ["--count", "0"],
        ["--count", "11"],
        ["--concurrency", "0"],
        ["--count", "2", "--connect", "--task-ids-file", "unread.txt"],
    ],
)
def test_invalid_options_fail_before_any_request(tracker: MockTracker, arguments: list[str]) -> None:
    result = _invoke(*arguments)

    assert result.exit_code != 0
    assert tracker.requests == []


@pytest.mark.parametrize(
    ("options", "sent"),
    [
        ([], (None, False, None, {})),
        (
            ["--provider", "modal", "--ics", "--priority", "0", "--header", "Authorization", "Bearer cli"],
            ("modal", True, 0, {"Authorization": "Bearer cli"}),
        ),
    ],
    ids=["defaults", "overrides"],
)
def test_start_sends_only_the_provider_and_priority_it_was_given(
    tracker: MockTracker, options: list[str], sent: tuple[object, ...]
) -> None:
    """Without --provider the Tracker uses the user's saved default provider, so the request leaves it out."""
    tracker.reply("POST", "/start-benchmark", _started(_FIRST_RUN_ID))

    result = _invoke(*options)

    assert result.exit_code == 0, result.output
    (body,) = _start_bodies(tracker)
    fields = ("sandbox_provider", "ignore_custom_services", "priority", "service_headers")
    assert tuple(body.get(field) for field in fields) == sent
    assert ("Priority override:" in result.output) == ("--priority" in options)


@pytest.mark.parametrize(
    ("count", "confirmed", "failure", "detail", "unknown_outcome"),
    [
        (3, 1, httpx.Response(403, json={"detail": "invalid token"}), "invalid token", False),
        (3, 1, httpx.Response(502, json={"detail": "benchmark unavailable"}), "benchmark unavailable", True),
        (3, 1, httpx.ConnectError("connection reset"), "connection reset", True),
        (2, 1, httpx.Response(200, json={"benchmark_id": str(_SECOND_RUN_ID)}), "StartBenchmarkResponse", True),
        (2, 0, httpx.Response(400, text="plain rejection"), "plain rejection", False),
        (1, 0, httpx.Response(503, text="tracker unavailable"), "tracker unavailable", True),
    ],
    ids=["authentication", "benchmark-service", "transport", "malformed-success", "first", "single"],
)
def test_failure_stops_and_reports_confirmed_progress(
    tracker: MockTracker,
    count: int,
    confirmed: int,
    failure: httpx.Response | Exception,
    detail: str,
    unknown_outcome: bool,
) -> None:
    """Stop at the first failed start; only a failure the server may have acted on is reported as unknown."""
    successes = [_started(run_id) for run_id in (_FIRST_RUN_ID,)[:confirmed]]
    tracker.reply("POST", "/start-benchmark", *successes, failure, _started(_THIRD_RUN_ID))

    result = _invoke(*([] if count == 1 else ["--count", str(count)]))

    assert result.exit_code == 1
    assert len(_start_bodies(tracker)) == confirmed + 1
    assert detail in result.output
    assert str(_THIRD_RUN_ID) not in result.output
    assert (_UNKNOWN_OUTCOME in result.output) is unknown_outcome
    if count > 1:
        assert f"{confirmed} / {count} requested runs successfully started." in result.output
    assert (f"valkyrie run status --ids {_FIRST_RUN_ID}" in result.output) is bool(confirmed)
