"""Run with `uv run pytest tests/unit/utils/test_failures.py`.

The task loop's retry decision, as a table: what each failure looks like and what happens to it.
"""

import pytest
from benchmark_service.client import (
    BenchmarkServiceError,
    BenchmarkServiceStreamError,
    BenchmarkServiceUnauthenticatedError,
)
from websockets.exceptions import ConnectionClosedError

from tracker.database.models import FailureCategory, FailurePhase
from tracker.exceptions import (
    AgentRunFailedError,
    OutputArtifactError,
    SandboxDNSResolutionError,
    SandboxError,
    SandboxSetupError,
)
from tracker.utils.failures import Disposition, decide, describe_failure


@pytest.mark.parametrize(
    ("exc", "phase", "expected"),
    [
        # An explicit setup failure always gets a fresh sandbox, even once the agent command has started.
        (SandboxSetupError("stream failed"), FailurePhase.AGENT, Disposition.RECREATE_SANDBOX),
        # Transient failures are only worth a fresh sandbox before model spend starts.
        (SandboxError("409 conflict", transient=True), FailurePhase.SETUP, Disposition.RECREATE_SANDBOX),
        (SandboxError("409 conflict", transient=True), FailurePhase.EVALUATION, Disposition.RESUME_EVALUATION),
        (SandboxError("409 conflict", transient=True), FailurePhase.AGENT, Disposition.TERMINAL),
        (
            BenchmarkServiceError("docker daemon is not ready inside the sandbox"),
            FailurePhase.SETUP,
            Disposition.RECREATE_SANDBOX,
        ),
        (
            BenchmarkServiceError("docker daemon is not ready inside the sandbox"),
            FailurePhase.AGENT,
            Disposition.TERMINAL,
        ),
        (BenchmarkServiceStreamError("closed"), FailurePhase.SETUP, Disposition.RECREATE_SANDBOX),
        (BenchmarkServiceStreamError("closed"), FailurePhase.EVALUATION, Disposition.RESUME_EVALUATION),
        (ConnectionClosedError(None, None), FailurePhase.EVALUATION, Disposition.RESUME_EVALUATION),
        # Deterministic failures never retry.
        (SandboxError("exit code 1"), FailurePhase.SETUP, Disposition.TERMINAL),
        (AgentRunFailedError("agent exited 1"), FailurePhase.AGENT, Disposition.TERMINAL),
        (SandboxDNSResolutionError("gateway"), FailurePhase.AGENT, Disposition.RECREATE_SANDBOX),
        (OutputArtifactError("missing"), FailurePhase.AGENT, Disposition.TERMINAL),
        (BenchmarkServiceError("bad request", status_code=400), FailurePhase.SETUP, Disposition.TERMINAL),
        (
            BenchmarkServiceUnauthenticatedError("bad key", status_code=401),
            FailurePhase.ADMISSION,
            Disposition.TERMINAL,
        ),
        # A benchmark-service error that does not say whether it is transient gets one fresh sandbox
        # before the agent, and none after it.
        (
            BenchmarkServiceError("Command failed: docker compose up"),
            FailurePhase.ADMISSION,
            Disposition.RECREATE_SANDBOX,
        ),
        (
            BenchmarkServiceError("Command failed: docker compose up"),
            FailurePhase.SANDBOX,
            Disposition.RECREATE_SANDBOX,
        ),
        (BenchmarkServiceError("Command failed: docker compose up"), FailurePhase.SETUP, Disposition.RECREATE_SANDBOX),
        (BenchmarkServiceError("Command failed: docker compose up"), FailurePhase.AGENT, Disposition.TERMINAL),
        (BenchmarkServiceError("Command failed: docker compose up"), FailurePhase.EVALUATION, Disposition.TERMINAL),
        (RuntimeError("boom"), FailurePhase.SETUP, Disposition.TERMINAL),
    ],
)
def test_decide_picks_the_cheapest_recovery(exc: Exception, phase: FailurePhase, expected: Disposition) -> None:
    assert decide(describe_failure(exc, str(exc), phase=phase, idle_seconds=0)) is expected


def _setup_error_with_cause(cause_code: str) -> SandboxSetupError:
    exc = SandboxSetupError("x")
    exc.cause_code = cause_code
    return exc


@pytest.mark.parametrize(
    ("exc", "category", "producer", "cause_code"),
    [
        (SandboxSetupError("x"), FailureCategory.INFRASTRUCTURE, "sandbox_provider", None),
        (
            _setup_error_with_cause("docker_not_ready"),
            FailureCategory.INFRASTRUCTURE,
            "sandbox_provider",
            "docker_not_ready",
        ),
        (AgentRunFailedError("x"), FailureCategory.AGENT, "tracker", None),
        (OutputArtifactError("x"), FailureCategory.INFRASTRUCTURE, "output_artifact", None),
        (
            BenchmarkServiceStreamError("x"),
            FailureCategory.BENCHMARK_SERVICE,
            "benchmark_service",
            "websocket_connection_closed",
        ),
        (
            BenchmarkServiceError("docker daemon is not ready inside the sandbox"),
            FailureCategory.BENCHMARK_SERVICE,
            "sandbox_provider",
            "docker_not_ready",
        ),
        (RuntimeError("x"), FailureCategory.UNKNOWN, "tracker", None),
    ],
)
def test_describe_failure_provenance(
    exc: Exception, category: FailureCategory, producer: str, cause_code: str | None
) -> None:
    failure = describe_failure(exc, str(exc), phase=FailurePhase.SETUP)
    assert (failure.category, failure.producer, failure.cause_code) == (category, producer, cause_code)


@pytest.mark.parametrize(
    ("exc", "transient"),
    [
        (BenchmarkServiceError("not found", status_code=404), False),
        (BenchmarkServiceUnauthenticatedError("bad key", status_code=401), False),
        (BenchmarkServiceError("upstream down", status_code=503), None),
        (BenchmarkServiceError("slow down", status_code=429), None),
        (BenchmarkServiceError("Command failed: docker compose up"), None),
        # A rejection wins over a Docker-not-ready signature in the message.
        (BenchmarkServiceError("docker daemon is not ready inside the sandbox", status_code=400), False),
    ],
)
def test_describe_failure_marks_rejected_benchmark_service_requests_not_transient(
    exc: BenchmarkServiceError, transient: bool | None
) -> None:
    assert describe_failure(exc, str(exc), phase=FailurePhase.SETUP).transient is transient
