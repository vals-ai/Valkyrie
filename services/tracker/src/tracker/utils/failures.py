"""Describe a caught task exception once, then decide what the task loop does with it.

Every ``except`` branch used to carry its own producer/operation/category literals and its
own retry decision. This module owns both: `describe_failure` turns an exception into a
`Failure`, and `decide` maps a `Failure` to a `Disposition`.
"""

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import NamedTuple

from benchmark_service.client import (
    BenchmarkServiceError,
    BenchmarkServiceStreamError,
    BenchmarkServiceUnauthenticatedError,
)
from benchmark_service.sandbox import SandboxError as ProviderSandboxError
from pydantic import ValidationError
from websockets.exceptions import ConnectionClosedError, InvalidStatus

from tracker.database.models import FailureCategory, FailurePhase
from tracker.exceptions import (
    AgentRunFailedError,
    CloudWatchError,
    LambdaError,
    OutputArtifactError,
    S3Error,
    SandboxDNSResolutionError,
    SandboxError,
    SandboxSetupError,
    SecretsError,
)

# A transient failure in these phases has not paid for model generation yet,
# so recreating the sandbox is cheap.
PRE_AGENT_PHASES = frozenset({FailurePhase.ADMISSION, FailurePhase.SANDBOX, FailurePhase.SETUP})


# Daytona sometimes schedules a sandbox onto a node whose Docker daemon never comes up.
# The benchmark service reports it as a plain request failure, so match the message.
_DOCKER_NOT_READY_SIGNATURES = (
    "docker daemon is not ready inside the sandbox",
    "Docker daemon did not become ready inside the compose sandbox",
)

_INFRASTRUCTURE_ERRORS = (
    SandboxError,
    ProviderSandboxError,
    OutputArtifactError,
    S3Error,
    CloudWatchError,
    LambdaError,
    SecretsError,
)


class Disposition(str, Enum):
    RECREATE_SANDBOX = "recreate_sandbox"
    RESUME_EVALUATION = "resume_evaluation"
    TERMINAL = "terminal"


@dataclass(frozen=True)
class Failure:
    exc: BaseException
    message: str
    category: FailureCategory
    producer: str
    operation: str
    phase: FailurePhase | None = None
    cause_code: str | None = None
    transient: bool | None = None

    @property
    def error_type(self) -> str:
        return type(self.exc).__name__


class BenchmarkServiceWebSocketDNSResolutionError(BenchmarkServiceError):
    """A benchmark-service WebSocket could not resolve its destination host."""


def classify_failure(exc: BaseException) -> FailureCategory:
    """Coarse category for places that only need it (run-level errors, telemetry)."""
    if isinstance(exc, asyncio.CancelledError):
        return FailureCategory.CANCELLED
    if isinstance(exc, AgentRunFailedError):
        return FailureCategory.AGENT
    if isinstance(exc, _INFRASTRUCTURE_ERRORS):
        return FailureCategory.INFRASTRUCTURE
    if isinstance(exc, BenchmarkServiceError):
        return FailureCategory.BENCHMARK_SERVICE
    return FailureCategory.UNKNOWN


class _Labels(NamedTuple):
    """The labels one exception type gets on its error row.

    ``transient=None`` keeps the exception's own flag; ``category=None`` keeps `classify_failure`'s.
    """

    producer: str
    operation: str
    transient: bool | None = None
    cause_code: str | None = None
    message: str = "{message}"
    category: FailureCategory | None = None


# Looked up along the exception's class chain, so a subclass's own entry wins over its base class's.
_LABELS: dict[type[BaseException], _Labels] = {
    SandboxSetupError: _Labels("sandbox_provider", "setup", transient=True),
    SandboxDNSResolutionError: _Labels("sandbox_provider", "dns", transient=True, cause_code="sandbox_dns_resolution"),
    OutputArtifactError: _Labels("output_artifact", "upload_output_artifacts", transient=False),
    AgentRunFailedError: _Labels("tracker", "process_task", transient=False),
    SandboxError: _Labels("sandbox_provider", "sandbox"),
    BenchmarkServiceWebSocketDNSResolutionError: _Labels(
        "benchmark_service",
        "websocket_connect",
        True,
        "websocket_dns_resolution",
        "Benchmark service WebSocket connection failed during DNS resolution: {message}",
    ),
    ConnectionClosedError: _Labels(
        "benchmark_service",
        "websocket",
        True,
        "websocket_connection_closed",
        "Benchmark service WebSocket disconnected: {message}; last application message received {idle_seconds}s ago",
        FailureCategory.BENCHMARK_SERVICE,
    ),
    BenchmarkServiceStreamError: _Labels(
        "benchmark_service",
        "websocket",
        True,
        "websocket_connection_closed",
        "Benchmark service WebSocket stream failed: {message}",
    ),
}


def describe_failure(
    exc: BaseException,
    message: str,
    *,
    phase: FailurePhase | None = None,
    idle_seconds: int | None = None,
) -> Failure:
    """Attach provenance and retry facts to an exception caught by the task loop.

    `message` is the already-rendered exception chain; `idle_seconds` is how long the
    benchmark-service stream had been silent, included for stream failures.
    """
    category = classify_failure(exc)
    transient = getattr(exc, "transient", None)
    labels = next((_LABELS[cls] for cls in type(exc).__mro__ if cls in _LABELS), None)
    if labels is not None:
        return Failure(
            exc,
            labels.message.format(message=message, idle_seconds=idle_seconds),
            labels.category or category,
            labels.producer,
            labels.operation,
            phase,
            cause_code=getattr(exc, "cause_code", None) or labels.cause_code,
            transient=transient if labels.transient is None else labels.transient,
        )
    if isinstance(exc, ValidationError):
        return _incompatible_response(exc, phase)
    if isinstance(exc, InvalidStatus):
        status = exc.response.status_code
        return Failure(
            exc,
            f"Benchmark service rejected the WebSocket connection (HTTP {status}): {message}",
            FailureCategory.BENCHMARK_SERVICE,
            "benchmark_service",
            "websocket_connect",
            phase,
            cause_code="websocket_http_rejected",
            transient=status >= 500,
        )
    if isinstance(exc, BenchmarkServiceError):
        return _benchmark_service_request(exc, message, category, phase)
    return Failure(exc, message, category, "tracker", "process_task", phase, transient=transient)


def _incompatible_response(exc: ValidationError, phase: FailurePhase | None) -> Failure:
    # str(ValidationError) embeds the rejected input values, so only locations and error types are kept.
    field_errors = ", ".join(
        f"{'.'.join(str(loc) for loc in err['loc'])} ({err['type']})"
        for err in exc.errors(include_url=False, include_context=False, include_input=False)
    )
    return Failure(
        exc,
        f"Benchmark service returned an incompatible task response. Missing or invalid fields: ValidationError: {field_errors}",
        FailureCategory.BENCHMARK_SERVICE,
        "benchmark_service",
        "decode_task_response",
        phase,
        cause_code="incompatible_response",
        transient=False,
    )


def _benchmark_service_request(
    exc: BenchmarkServiceError, message: str, category: FailureCategory, phase: FailurePhase | None
) -> Failure:
    # A rejected request (bad credentials, unknown task, unsupported dataset) fails the same way again.
    # A timeout (408) or rate limit (429) is a 4xx that can clear on its own.
    status = exc.status_code
    if isinstance(exc, BenchmarkServiceUnauthenticatedError) or (
        status is not None and 400 <= status < 500 and status not in (408, 429)
    ):
        return Failure(exc, message, category, "benchmark_service", "request", phase, transient=False)
    if any(signature in message for signature in _DOCKER_NOT_READY_SIGNATURES):
        return Failure(
            exc, message, category, "sandbox_provider", "setup", phase, cause_code="docker_not_ready", transient=True
        )
    return Failure(exc, message, category, "benchmark_service", "request", phase)


def decide(failure: Failure) -> Disposition:
    """Pick the cheapest recovery that can still succeed.

    An explicit `SandboxSetupError` always gets a fresh sandbox: the raiser has already
    established the sandbox itself is unusable. So does a `SandboxDNSResolutionError`, raised
    when the tracker finds the sandbox cannot resolve the agent's gateway after the agent
    failed; the rerun pays for the agent run again. Any other transient failure gets one only
    before the agent runs, when nothing has been paid for yet. A benchmark-service error that
    does not say whether it is transient (e.g. ``Command failed: docker compose`` from
    setup_task) gets one too in that window, since a rerun costs only setup time. After that, a transient failure may resume evaluation if
    the service persisted continuation state.
    """
    if isinstance(failure.exc, (SandboxSetupError, SandboxDNSResolutionError)):
        return Disposition.RECREATE_SANDBOX
    if failure.phase in PRE_AGENT_PHASES and (
        failure.transient or (failure.transient is None and failure.category is FailureCategory.BENCHMARK_SERVICE)
    ):
        return Disposition.RECREATE_SANDBOX
    if not failure.transient:
        return Disposition.TERMINAL
    if failure.phase is FailurePhase.EVALUATION:
        return Disposition.RESUME_EVALUATION
    return Disposition.TERMINAL
