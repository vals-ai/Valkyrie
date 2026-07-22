"""Tests for counted run starts.

Run: uv run pytest tests/unit/cli/run/test_start.py -v

Covers count validation, successful starts, contract reuse, and failure reporting.
"""

from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner, Result
from tracker.agent.schemas import AgentConfig
from tracker.database.models import AgentContractRequest, BenchmarkStatus, TaskStatus

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.progress import stream_benchmark_status

from tests.unit.cli.factories import make_fetch_response

start_module = import_module("valkyrie.cli.run.start")
start_command = start_module.start

_FIRST_RUN_ID = UUID("00000000-0000-0000-0000-000000000001")
_SECOND_RUN_ID = UUID("00000000-0000-0000-0000-000000000002")
_THIRD_RUN_ID = UUID("00000000-0000-0000-0000-000000000003")
_STARTED_AT = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
_UNKNOWN_OUTCOME = "outcome may be unknown"


def _start_response(run_id: UUID) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "benchmark_name": "swebench",
            "agent_name": "remote-agent",
            "benchmark_id": str(run_id),
            "concurrency": 5,
            "started_at": _STARTED_AT.isoformat(),
            "task_count": 2,
            "cloudwatch_url": f"https://cloudwatch.example/{run_id}",
            "s3_bucket_url": f"s3://runs/{run_id}",
        },
    )


class StartTestbed:
    """Hold the command runner and mocked external boundaries."""

    def __init__(self, cli_runner: CliRunner) -> None:
        self.cli_runner = cli_runner
        self.tracker = MagicMock()
        self.tracker.__enter__.return_value = self.tracker
        self.tracker_factory = MagicMock(return_value=self.tracker)
        self.tracker_factory.validate_sandbox_provider.return_value = ("daytona", "DaytonaSecrets")
        self.tracker_factory.get_webhook_secret.return_value = None
        self.resolve_remote = AsyncMock(
            return_value=AgentContractRequest(name="remote-agent", install_cmd="echo install", run_cmd="echo run")
        )
        self.resolve_tasks = MagicMock(return_value=None)
        self.resolve_headers = MagicMock(return_value={})
        self.stream_status = MagicMock()

    def set_responses(self, responses: list[httpx.Response | TrackerServiceError]) -> None:
        for boundary in (
            self.tracker,
            self.tracker_factory,
            self.resolve_remote,
            self.resolve_tasks,
            self.resolve_headers,
            self.stream_status,
        ):
            boundary.reset_mock()
        self.tracker.start_benchmark.side_effect = responses

    def invoke(self, arguments: list[str]) -> Result:
        return self.cli_runner.invoke(
            start_command,
            ["--agent", "remote-agent", "--benchmark", "swebench", *arguments],
        )

    def boundary_call_count(self) -> int:
        return sum(
            boundary.call_count
            for boundary in (
                self.tracker_factory,
                self.tracker_factory.validate_sandbox_provider,
                self.tracker_factory.get_webhook_secret,
                self.resolve_remote,
                self.resolve_tasks,
                self.resolve_headers,
            )
        )


@pytest.fixture
def start_testbed(monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner) -> StartTestbed:
    """Replace external start-command boundaries with deterministic mocks."""
    testbed = StartTestbed(cli_runner)
    monkeypatch.setattr(start_module, "TrackerService", testbed.tracker_factory)
    monkeypatch.setattr(start_module, "get_contract_from_s3", testbed.resolve_remote)
    monkeypatch.setattr(start_module, "resolve_task_ids", testbed.resolve_tasks)
    monkeypatch.setattr(start_module, "benchmark_service_headers", testbed.resolve_headers)
    monkeypatch.setattr(start_module, "stream_benchmark_status", testbed.stream_status)
    testbed.set_responses([_start_response(_FIRST_RUN_ID)])

    return testbed


class TestCountedStarts:
    """Public CLI behavior for successful and rejected counted starts."""

    def test_single_start_remains_compatible(self, start_testbed: StartTestbed) -> None:
        """
        Preserve the existing one-run command while documenting the new option.

        Test cases:
        - Omitting count starts one run without a batch summary.
        - Explicit count one still supports connected streaming.
        """
        # Invoke the unchanged default behavior.
        result = start_testbed.invoke([])

        assert result.exit_code == 0, result.output
        assert start_testbed.tracker.start_benchmark.call_count == 1
        assert "requested runs successfully started" not in result.output

        # Exercise the short alias with the compatible connect mode.
        start_testbed.set_responses([_start_response(_FIRST_RUN_ID)])
        connected_result = start_testbed.invoke(["-n", "1", "--connect"])

        assert connected_result.exit_code == 0, connected_result.output
        start_testbed.stream_status.assert_called_once_with(start_testbed.tracker, _FIRST_RUN_ID)

    def test_connected_start_survives_task_discovery(
        self,
        start_testbed: StartTestbed,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A confirmed start must stay connected while task rows are being discovered.

        Test cases:
        - The initial empty status is rendered as task discovery without losing the run ID.
        - Later task progress and terminal completion are consumed from the same stream.
        """
        initial_response = make_fetch_response(
            _FIRST_RUN_ID,
            status=BenchmarkStatus.IN_PROGRESS,
            total_tasks=0,
            finished_tasks=0,
            task_breakdown={},
        )
        discovered_response = make_fetch_response(
            _FIRST_RUN_ID,
            status=BenchmarkStatus.IN_PROGRESS,
            total_tasks=2,
            finished_tasks=0,
            task_breakdown={TaskStatus.PENDING: 1, TaskStatus.BUILDING: 1},
        )
        completed_response = make_fetch_response(
            _FIRST_RUN_ID,
            status=BenchmarkStatus.FINISHED,
            total_tasks=2,
            finished_tasks=2,
            task_breakdown={TaskStatus.FINISHED: 2},
        )
        start_testbed.tracker.fetch_benchmark.return_value = initial_response
        start_testbed.tracker.stream_benchmark.return_value = iter(
            [
                f"data: {discovered_response.model_dump_json()}",
                f"data: {completed_response.model_dump_json()}",
                "event: complete",
            ]
        )
        monkeypatch.setattr(start_module, "stream_benchmark_status", stream_benchmark_status)

        result = start_testbed.invoke(["--connect"])

        assert result.exit_code == 0, result.output
        assert str(_FIRST_RUN_ID) in result.output
        assert "0/0 (0.0%)" in result.output
        assert "Pending: 1" in result.output
        assert "2/2 (100.0%)" in result.output

    def test_counted_start_reuses_contract_and_reports_ids(
        self,
        start_testbed: StartTestbed,
    ) -> None:
        """
        Start ordered independent runs from one resolved contract.

        Test cases:
        - The long count option starts the requested runs through one tracker context.
        - The existing `-k` option reaches the one reused contract.
        - Output includes every run ID and the combined status command.
        """
        # Arrange two successful starts.
        start_testbed.set_responses([_start_response(_FIRST_RUN_ID), _start_response(_SECOND_RUN_ID)])

        # Invoke the counted command with an existing kwarg and stable label.
        result = start_testbed.invoke(["-k", "temperature", "1", "--label", "stable", "--count", "2"])

        assert result.exit_code == 0, result.output
        assert start_testbed.tracker.start_benchmark.call_count == 2
        assert start_testbed.resolve_remote.await_count == 1
        assert start_testbed.tracker.__enter__.call_count == 1

        resolved_call = start_testbed.resolve_remote.await_args
        assert resolved_call is not None

        agent_config = resolved_call.args[1]
        assert isinstance(agent_config, AgentConfig)
        assert agent_config.kwargs == {"temperature": "1"}

        start_requests = start_testbed.tracker.start_benchmark.call_args_list
        assert start_requests[0].args[0] is start_requests[1].args[0]
        assert [request.args[6] for request in start_requests] == ["stable", "stable"]

        details, summary = result.output.split("2 / 2 requested runs successfully started.", maxsplit=1)
        expected_ids = f"{_FIRST_RUN_ID},{_SECOND_RUN_ID}"

        assert str(_FIRST_RUN_ID) in details
        assert str(_SECOND_RUN_ID) in details
        assert f"Track progress: valkyrie run status --ids {expected_ids}" in summary
        assert "Confirmed started run IDs:" not in result.output

    def test_counted_local_start_uploads_once(
        self,
        start_testbed: StartTestbed,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """
        Resolve and upload a local agent once before counted starts.

        Test cases:
        - The local contract is read once.
        - The local agent is uploaded once while two runs start.
        """
        # Arrange a local agent and its external boundaries.
        local_agent = tmp_path / "local-agent"
        local_agent.mkdir()
        contract_file = local_agent / "contract.yaml"
        contract_file.write_text("name: local-agent\n", encoding="utf-8")
        get_contract = MagicMock(
            return_value=AgentContractRequest(name="local-agent", install_cmd="echo install", run_cmd="echo run")
        )
        push_agent = AsyncMock()
        monkeypatch.setattr(start_module, "get_contract", get_contract)
        monkeypatch.setattr(start_module, "push_agent", push_agent)
        start_testbed.set_responses([_start_response(_FIRST_RUN_ID), _start_response(_SECOND_RUN_ID)])

        # Start two runs from the local agent.
        result = start_testbed.cli_runner.invoke(
            start_command,
            ["--agent", str(local_agent), "--benchmark", "swebench", "--count", "2"],
        )

        assert result.exit_code == 0, result.output
        get_contract.assert_called_once()
        push_agent.assert_awaited_once_with("local-agent", local_agent)
        assert start_testbed.tracker.start_benchmark.call_count == 2

    def test_invalid_options_precede_side_effects(self, start_testbed: StartTestbed) -> None:
        """
        Reject invalid start options before resolving configuration or agents.

        Test cases:
        - Counts below one and above ten fail Click range validation.
        - Concurrency below one fails Click range validation.
        - Connect with multiple starts fails before command side effects.
        """
        cases = [
            ["--count", "0"],
            ["--count", "11"],
            ["--concurrency", "0"],
            ["--concurrency", "-1"],
            ["--count", "2", "--connect", "--task-ids-file", "unread.txt"],
        ]

        # Invoke each rejected option combination from a clean boundary state.
        for arguments in cases:
            start_testbed.set_responses([_start_response(_FIRST_RUN_ID)])
            result = start_testbed.invoke(arguments)

            assert result.exit_code == 2
            assert start_testbed.boundary_call_count() == 0

    @pytest.mark.parametrize(
        ("count", "prior_ids", "failure", "expected_detail", "unknown_outcome"),
        [
            pytest.param(
                3,
                (_FIRST_RUN_ID,),
                httpx.Response(403, json={"detail": "invalid token"}),
                "invalid token",
                False,
                id="authentication",
            ),
            pytest.param(
                3,
                (_FIRST_RUN_ID,),
                httpx.Response(502, json={"detail": "benchmark unavailable"}),
                "benchmark unavailable",
                True,
                id="benchmark-service",
            ),
            pytest.param(
                3,
                (_FIRST_RUN_ID,),
                TrackerServiceError("connection reset"),
                "connection reset",
                True,
                id="transport",
            ),
            pytest.param(
                2,
                (_FIRST_RUN_ID,),
                httpx.Response(200, json={"benchmark_id": str(_SECOND_RUN_ID)}),
                "malformed",
                True,
                id="malformed-success",
            ),
            pytest.param(2, (), httpx.Response(400, text="plain rejection"), "plain rejection", False, id="first"),
            pytest.param(
                1, (), httpx.Response(503, text="tracker unavailable"), "tracker unavailable", True, id="single"
            ),
        ],
    )
    def test_failure_stops_and_reports_progress(
        self,
        start_testbed: StartTestbed,
        count: int,
        prior_ids: tuple[UUID, ...],
        failure: httpx.Response | TrackerServiceError,
        expected_detail: str,
        unknown_outcome: bool,
    ) -> None:
        """
        Stop at the first failed request and report only confirmed progress.

        Test cases:
        - JSON, plain-text, transport, and malformed-success failures exit nonzero.
        - Confirmed prefixes remain queryable and zero-prefix batches omit an empty command.
        - Uncertain outcomes direct users to list runs.
        """
        # Arrange confirmed responses, the failure, and an unreachable later response.
        responses: list[httpx.Response | TrackerServiceError] = [_start_response(run_id) for run_id in prior_ids]
        responses.extend([failure, _start_response(_THIRD_RUN_ID)])
        start_testbed.set_responses(responses)

        # Start the requested runs.
        arguments = [] if count == 1 else ["--count", str(count)]
        result = start_testbed.invoke(arguments)

        assert result.exit_code != 0
        assert start_testbed.tracker.start_benchmark.call_count == len(prior_ids) + 1
        assert expected_detail in result.output
        assert "Authentication error:" not in result.output
        assert "Benchmark service error:" not in result.output
        assert str(_THIRD_RUN_ID) not in result.output
        assert (_UNKNOWN_OUTCOME in result.output) is unknown_outcome
        assert ("valkyrie run list" in result.output) is unknown_outcome

        if prior_ids:
            confirmed_ids = ",".join(str(run_id) for run_id in prior_ids)

            assert f"{len(prior_ids)} / {count} requested runs successfully started." in result.output
            assert f"Track progress: valkyrie run status --ids {confirmed_ids}" in result.output
        elif count > 1:
            assert f"0 / {count} requested runs successfully started." in result.output
            assert "valkyrie run status --ids" not in result.output
        else:
            assert "requested runs successfully started" not in result.output
