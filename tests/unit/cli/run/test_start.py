"""Tests for counted run starts.

Run: uv run pytest tests/unit/cli/run/test_start.py -v

Covers counted CLI starts, contract reuse, failure reporting, and connect validation.
"""

from collections.abc import Sequence
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner, Result
from tracker.agent.schemas import AgentConfig
from tracker.database.models import AgentContractRequest

from valkyrie.cli.exceptions import TrackerServiceError

start_module = import_module("valkyrie.cli.run.start")
start_command = start_module.start

_FIRST_RUN_ID = UUID("00000000-0000-0000-0000-000000000001")
_SECOND_RUN_ID = UUID("00000000-0000-0000-0000-000000000002")
_THIRD_RUN_ID = UUID("00000000-0000-0000-0000-000000000003")
_STARTED_AT = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)


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


class MockTrackerService:
    """Record tracker lifecycle and start requests made by the command."""

    responses: list[httpx.Response | TrackerServiceError] = []
    start_calls: list[dict[str, object]] = []
    init_calls = 0
    enter_calls = 0
    provider_validations: list[str | None] = []

    def __init__(self) -> None:
        self.__class__.init_calls += 1

    def __enter__(self) -> "MockTrackerService":
        self.__class__.enter_calls += 1

        return self

    def __exit__(self, *_exc_info: object) -> None:
        return None

    @classmethod
    def validate_sandbox_provider(cls, provider: str | None = None) -> tuple[str, str]:
        cls.provider_validations.append(provider)

        return provider or "daytona", "DaytonaSecrets"

    @staticmethod
    def get_webhook_secret() -> None:
        return None

    def start_benchmark(
        self,
        contract: AgentContractRequest,
        benchmark_name: str,
        concurrency: int,
        ignore_custom_services: bool,
        task_ids: list[str] | None,
        slice_str: str | None,
        label: str | None = None,
        lambda_function: str | None = None,
        dataset: str | None = None,
        service_headers: dict[str, str] | None = None,
        provider: str | None = None,
        webhook_secret_name: str | None = None,
        webhook_intervals: list[int] | None = None,
    ) -> httpx.Response:
        self.start_calls.append(
            {
                "contract": contract,
                "benchmark_name": benchmark_name,
                "concurrency": concurrency,
                "ignore_custom_services": ignore_custom_services,
                "task_ids": task_ids,
                "slice_str": slice_str,
                "label": label,
                "lambda_function": lambda_function,
                "dataset": dataset,
                "service_headers": service_headers,
                "provider": provider,
                "webhook_secret_name": webhook_secret_name,
                "webhook_intervals": webhook_intervals,
            }
        )
        response = self.responses[len(self.start_calls) - 1]
        if isinstance(response, TrackerServiceError):
            raise response

        return response


class StartTestbed:
    """Hold observable boundary activity for one CLI test."""

    def __init__(self, cli_runner: CliRunner) -> None:
        self.cli_runner = cli_runner
        self.remote_resolutions: list[tuple[str, AgentConfig]] = []
        self.task_resolutions: list[tuple[str | None, str | None]] = []
        self.service_header_resolutions: list[tuple[str, tuple[tuple[str, str], ...]]] = []
        self.streamed_run_ids: list[UUID] = []

    def set_responses(self, responses: Sequence[httpx.Response | TrackerServiceError]) -> None:
        MockTrackerService.responses = list(responses)
        MockTrackerService.start_calls = []
        MockTrackerService.init_calls = 0
        MockTrackerService.enter_calls = 0
        MockTrackerService.provider_validations = []

    def invoke(self, arguments: list[str]) -> Result:
        return self.cli_runner.invoke(
            start_command,
            ["--agent", "remote-agent", "--benchmark", "swebench", *arguments],
        )


@pytest.fixture
def cli_runner() -> CliRunner:
    """Provide an isolated Click runner for each test."""

    return CliRunner()


@pytest.fixture
def start_testbed(monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner) -> StartTestbed:
    """Isolate the command from tracker, S3, task-file, and config boundaries."""
    testbed = StartTestbed(cli_runner)

    async def get_contract_from_s3(agent: str, agent_config: AgentConfig) -> AgentContractRequest:
        testbed.remote_resolutions.append((agent, agent_config))

        return AgentContractRequest(name=agent, install_cmd="echo install", run_cmd="echo run")

    def resolve_task_ids(task_ids: str | None, task_ids_file: str | None) -> list[str] | None:
        testbed.task_resolutions.append((task_ids, task_ids_file))

        return ["task-a", "task-b"] if task_ids or task_ids_file else None

    def benchmark_service_headers(
        benchmark_name: str,
        headers: tuple[tuple[str, str], ...],
    ) -> dict[str, str]:
        testbed.service_header_resolutions.append((benchmark_name, headers))

        return dict(headers)

    def stream_benchmark_status(_tracker: MockTrackerService, run_id: UUID) -> None:
        testbed.streamed_run_ids.append(run_id)

    monkeypatch.setattr(start_module, "TrackerService", MockTrackerService)
    monkeypatch.setattr(start_module, "get_contract_from_s3", get_contract_from_s3)
    monkeypatch.setattr(start_module, "resolve_task_ids", resolve_task_ids)
    monkeypatch.setattr(start_module, "benchmark_service_headers", benchmark_service_headers)
    monkeypatch.setattr(start_module, "stream_benchmark_status", stream_benchmark_status)
    testbed.set_responses([_start_response(_FIRST_RUN_ID)])

    return testbed


class TestCountedStarts:
    """Successful counted starts and one-time contract resolution."""

    def test_count_one_defaults_explicitly_and_connects(self, start_testbed: StartTestbed) -> None:
        """Preserve single-run defaults and connected streaming.

        Test cases:
        - Omitting count starts exactly one run.
        - Explicit count one starts exactly one run.
        - Count one with connect streams the accepted run.
        - Help displays the count default and accepted range.
        """
        # Run each single-start form against a fresh response.
        results: list[Result] = []
        for arguments in ([], ["--count", "1"], ["-n", "1", "--connect"]):
            start_testbed.set_responses([_start_response(_FIRST_RUN_ID)])

            results.append(start_testbed.invoke(arguments))

            assert len(MockTrackerService.start_calls) == 1, results[-1].output + repr(results[-1].exception)

        # Confirm all forms succeeded and only connect streamed.
        assert all(result.exit_code == 0 for result in results)
        assert start_testbed.streamed_run_ids == [_FIRST_RUN_ID]
        assert "Track progress:" not in results[-1].output

        # Inspect the count option shown to users.
        help_result = start_testbed.cli_runner.invoke(start_command, ["--help"])

        assert help_result.exit_code == 0
        assert "-n, --count INTEGER RANGE" in help_result.output
        assert "default:" in help_result.output
        assert "1; 1<=x<=10" in help_result.output

    @pytest.mark.parametrize("count_option", ["--count", "-n"])
    def test_count_aliases_start_ordered_independent_runs(
        self,
        start_testbed: StartTestbed,
        count_option: str,
    ) -> None:
        """Start each requested run in order while reusing resolved configuration.

        Test cases:
        - Both count option spellings start two independent runs.
        - Agent kwargs remain owned by `-k` and reach one shared resolved contract.
        - Output lists each run and the ordered combined status command.
        """
        # Arrange two accepted tracker responses in a fixed order.
        start_testbed.set_responses([_start_response(_FIRST_RUN_ID), _start_response(_SECOND_RUN_ID)])

        # Start the requested independent runs through the public command.
        result = start_testbed.invoke(["-k", "temperature", "1", "--label", "stable-label", count_option, "2"])

        # Verify the tracker boundary reused one resolved contract and label.
        assert result.exit_code == 0, result.output
        assert len(MockTrackerService.start_calls) == 2
        assert MockTrackerService.init_calls == 1
        assert MockTrackerService.enter_calls == 1
        assert len(start_testbed.remote_resolutions) == 1

        agent_config = start_testbed.remote_resolutions[0][1]
        assert agent_config.kwargs == {"temperature": "1"}

        contracts = [call["contract"] for call in MockTrackerService.start_calls]

        assert contracts[0] is contracts[1]
        assert [call["label"] for call in MockTrackerService.start_calls] == ["stable-label", "stable-label"]

        # Isolate individual details from the final combined summary.
        details_output, summary_output = result.output.split("Confirmed started run IDs:", maxsplit=1)
        run_id_rows = [line for line in details_output.splitlines() if line.startswith("│ Run ID:")]
        expected_run_ids = f"{_FIRST_RUN_ID},{_SECOND_RUN_ID}"

        # Require one ordered Run ID row per response before the summary.
        assert details_output.count("┌─ Run Details ") == 2
        assert run_id_rows == [
            f"│ {'Run ID:':<17} {_FIRST_RUN_ID}",
            f"│ {'Run ID:':<17} {_SECOND_RUN_ID}",
        ]
        assert summary_output.startswith(f" {expected_run_ids}")
        assert f"valkyrie run status --ids {expected_run_ids}" in summary_output

    def test_local_and_remote_contracts_resolve_once(
        self, start_testbed: StartTestbed, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Resolve either agent source once before multiple starts.

        Test cases:
        - A remote contract is downloaded once and reused.
        - A local contract is built and uploaded once and reused.
        """
        # Arrange two successful remote-agent starts.
        start_testbed.set_responses([_start_response(_FIRST_RUN_ID), _start_response(_SECOND_RUN_ID)])

        # Start the remote agent twice.
        remote_result = start_testbed.invoke(["--count", "2"])

        # Confirm the remote contract was resolved once.
        assert remote_result.exit_code == 0, remote_result.output
        assert len(start_testbed.remote_resolutions) == 1

        # Arrange a local agent and record its contract/upload boundaries.
        local_agent = tmp_path / "local-agent"
        local_agent.mkdir()
        (local_agent / "contract.yaml").write_text("name: local-agent\n", encoding="utf-8")
        contract_resolutions: list[tuple[Path, AgentConfig]] = []
        uploads: list[tuple[str, Path]] = []

        def get_contract(contract_file: Path, agent_config: AgentConfig) -> AgentContractRequest:
            contract_resolutions.append((contract_file, agent_config))

            return AgentContractRequest(name="local-agent", install_cmd="echo install", run_cmd="echo run")

        async def push_agent(agent_name: str, agent_path: Path) -> None:
            uploads.append((agent_name, agent_path))

        monkeypatch.setattr(start_module, "get_contract", get_contract)
        monkeypatch.setattr(start_module, "push_agent", push_agent)
        start_testbed.set_responses([_start_response(_FIRST_RUN_ID), _start_response(_SECOND_RUN_ID)])

        # Start the local agent twice through the same runner fixture.
        local_result = start_testbed.cli_runner.invoke(
            start_command,
            ["--agent", str(local_agent), "--benchmark", "swebench", "--count", "2"],
        )

        # Confirm local resolution and upload each happened once.
        assert local_result.exit_code == 0, local_result.output
        assert len(contract_resolutions) == 1
        assert uploads == [("local-agent", local_agent)]
        assert len(MockTrackerService.start_calls) == 2


class TestCountValidation:
    """Count parsing and early connect rejection."""

    @pytest.mark.parametrize("count", ["0", "11", "many"])
    def test_invalid_counts_fail_before_side_effects(self, start_testbed: StartTestbed, count: str) -> None:
        """Reject out-of-range and non-integer counts during Click parsing.

        Test cases:
        - Zero and eleven fail the configured range.
        - Non-integer input fails integer parsing.
        - Validation occurs before command side effects.
        """
        # Invoke the command with an invalid count value.
        result = start_testbed.invoke(["--count", count])

        # Confirm Click rejected it before reaching any boundary.
        assert result.exit_code == 2
        assert not start_testbed.task_resolutions
        assert not start_testbed.service_header_resolutions
        assert not start_testbed.remote_resolutions
        assert MockTrackerService.init_calls == 0

    def test_multiple_connected_starts_fail_before_side_effects(self, start_testbed: StartTestbed) -> None:
        """Reject connect with multiple starts at the callback boundary.

        Test cases:
        - The conflict exits with Click usage status two.
        - Task, config, provider, agent, and tracker boundaries remain untouched.
        """
        # Supply options that would cause side effects if the guard ran late.
        result = start_testbed.invoke(
            ["--count", "2", "--connect", "--task-ids-file", "must-not-be-read.txt", "--provider", "modal"]
        )

        # Confirm the callback rejected the conflict before side effects.
        assert result.exit_code == 2
        assert "--connect" in result.output
        assert not start_testbed.task_resolutions
        assert not start_testbed.service_header_resolutions
        assert not start_testbed.remote_resolutions
        assert not MockTrackerService.provider_validations
        assert MockTrackerService.init_calls == 0


class TestStartFailures:
    """Partial progress and uncertain start outcomes."""

    @pytest.mark.parametrize(
        ("response", "expected_category"),
        [
            pytest.param(
                httpx.Response(403, json={"detail": "invalid token"}),
                "Authentication error: invalid token",
                id="authentication",
            ),
            pytest.param(
                httpx.Response(502, json={"detail": "benchmark unavailable"}),
                "Benchmark service error: benchmark unavailable",
                id="benchmark-service",
            ),
        ],
    )
    def test_later_http_failure_stops_and_reports_confirmed_ids(
        self,
        start_testbed: StartTestbed,
        response: httpx.Response,
        expected_category: str,
    ) -> None:
        """Stop a batch at its first rejected tracker response.

        Test cases:
        - Authentication and benchmark-service detail categories are preserved.
        - Prior confirmed IDs and their status command are reported.
        - No request is made after the failure.
        """
        # Arrange one success, one rejection, and an unreachable response.
        start_testbed.set_responses([_start_response(_FIRST_RUN_ID), response, _start_response(_THIRD_RUN_ID)])

        # Start the batch through the command.
        result = start_testbed.invoke(["--count", "3"])

        # Confirm partial progress and immediate termination are visible.
        assert result.exit_code != 0
        assert len(MockTrackerService.start_calls) == 2
        assert expected_category in result.output
        assert f"Confirmed started run IDs: {_FIRST_RUN_ID}" in result.output
        assert f"valkyrie run status --ids {_FIRST_RUN_ID}" in result.output
        assert str(_THIRD_RUN_ID) not in result.output

    def test_transport_failure_reports_unknown_latest_outcome(self, start_testbed: StartTestbed) -> None:
        """Preserve confirmed progress when a later tracker request loses its response.

        Test cases:
        - A tracker transport error stops later requests.
        - The prior run remains confirmed and queryable.
        - The latest outcome is marked unknown with run-list verification guidance.
        """
        # Arrange a transport failure after one confirmed start.
        start_testbed.set_responses(
            [_start_response(_FIRST_RUN_ID), TrackerServiceError("connection reset"), _start_response(_THIRD_RUN_ID)]
        )

        # Start the batch through the command.
        result = start_testbed.invoke(["--count", "3"])

        # Confirm prior progress and uncertain-outcome guidance are shown.
        assert result.exit_code != 0
        assert len(MockTrackerService.start_calls) == 2
        assert f"Confirmed started run IDs: {_FIRST_RUN_ID}" in result.output
        assert f"valkyrie run status --ids {_FIRST_RUN_ID}" in result.output
        assert "outcome may be unknown" in result.output
        assert "valkyrie run list" in result.output

    @pytest.mark.parametrize(
        ("response", "expected_detail"),
        [
            pytest.param(httpx.Response(400, json={"detail": "invalid request"}), "invalid request", id="json"),
            pytest.param(httpx.Response(400, text="plain rejection"), "plain rejection", id="text"),
        ],
    )
    def test_first_batch_failure_reports_zero_without_empty_command(
        self,
        start_testbed: StartTestbed,
        response: httpx.Response,
        expected_detail: str,
    ) -> None:
        """Report an empty confirmed prefix without suggesting an invalid command.

        Test cases:
        - JSON and plain-text error detail are rendered safely.
        - A first-request failure reports zero confirmed starts.
        - No empty combined status command is printed.
        """
        # Arrange a rejection for the first requested start.
        start_testbed.set_responses([response])

        # Start a two-run batch through the command.
        result = start_testbed.invoke(["--count", "2"])

        # Confirm useful detail without an invalid empty status command.
        assert result.exit_code != 0
        assert expected_detail in result.output
        assert "zero confirmed starts" in result.output
        assert "valkyrie run status --ids" not in result.output

    def test_malformed_success_reports_prior_ids_and_unknown_outcome(self, start_testbed: StartTestbed) -> None:
        """Handle a 200 response that lacks a usable start payload.

        Test cases:
        - Prior accepted run IDs remain confirmed.
        - The malformed response exits cleanly without another request.
        - The latest outcome is marked unknown with run-list verification guidance.
        """
        # Arrange one valid response followed by a malformed success body.
        start_testbed.set_responses(
            [_start_response(_FIRST_RUN_ID), httpx.Response(200, json={"benchmark_id": str(_SECOND_RUN_ID)})]
        )

        # Start the two-run batch through the command.
        result = start_testbed.invoke(["--count", "2"])

        # Confirm prior progress and malformed-response guidance are shown.
        assert result.exit_code != 0
        assert len(MockTrackerService.start_calls) == 2
        assert f"Confirmed started run IDs: {_FIRST_RUN_ID}" in result.output
        assert f"valkyrie run status --ids {_FIRST_RUN_ID}" in result.output
        assert "malformed" in result.output.lower()
        assert "outcome may be unknown" in result.output
        assert "valkyrie run list" in result.output

    def test_single_non_json_server_error_exits_nonzero(self, start_testbed: StartTestbed) -> None:
        """Treat every single-run non-200 response as a command failure.

        Test cases:
        - Plain server text remains useful error detail.
        - Count one exits nonzero.
        - A server response warns that acceptance may be unknown.
        """
        # Arrange a plain-text server error.
        start_testbed.set_responses([httpx.Response(503, text="tracker unavailable")])

        # Start the default single run.
        result = start_testbed.invoke([])

        # Confirm clean failure detail and uncertain-outcome guidance.
        assert result.exit_code != 0
        assert "tracker unavailable" in result.output
        assert "outcome may be unknown" in result.output
        assert "valkyrie run list" in result.output
