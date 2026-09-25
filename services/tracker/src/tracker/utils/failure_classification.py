"""Map exceptions caught by catch-all handlers onto a FailureCategory."""

import asyncio

from benchmark_service.client import BenchmarkServiceError
from benchmark_service.sandbox import SandboxError as ProviderSandboxError

from tracker.database.models import FailureCategory
from tracker.exceptions import (
    AgentRunFailedError,
    CloudWatchError,
    LambdaError,
    OutputArtifactError,
    S3Error,
    SandboxError,
    SecretsError,
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


def classify_failure(exc: BaseException) -> FailureCategory:
    if isinstance(exc, asyncio.CancelledError):
        return FailureCategory.CANCELLED
    if isinstance(exc, AgentRunFailedError):
        return FailureCategory.AGENT
    if isinstance(exc, _INFRASTRUCTURE_ERRORS):
        return FailureCategory.INFRASTRUCTURE
    if isinstance(exc, BenchmarkServiceError):
        return FailureCategory.BENCHMARK_SERVICE
    return FailureCategory.UNKNOWN
