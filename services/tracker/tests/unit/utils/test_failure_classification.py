"""Run with `uv run pytest tests/unit/utils/test_failure_classification.py`.

Cover the catch-all exception-to-category mapping.
"""

import asyncio

import pytest
from benchmark_service.client import BenchmarkServiceError

from tracker.database.models import FailureCategory
from tracker.exceptions import AgentRunFailedError, OutputArtifactError, S3Error, SandboxError
from tracker.utils.failure_classification import classify_failure


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (AgentRunFailedError("agent exited 1"), FailureCategory.AGENT),
        (SandboxError("sandbox gone"), FailureCategory.INFRASTRUCTURE),
        (OutputArtifactError("upload failed"), FailureCategory.INFRASTRUCTURE),
        (S3Error("s3 failed"), FailureCategory.INFRASTRUCTURE),
        (BenchmarkServiceError("stream closed"), FailureCategory.BENCHMARK_SERVICE),
        (asyncio.CancelledError(), FailureCategory.CANCELLED),
        (RuntimeError("boom"), FailureCategory.UNKNOWN),
    ],
)
def test_classify_failure_maps_exception_to_category(exc: BaseException, expected: FailureCategory) -> None:
    """Agent failures must win over their SandboxError base class; unrecognized errors stay unknown."""
    assert classify_failure(exc) == expected
