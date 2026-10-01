"""Unit tests for the per-benchmark agent freeze helper.

Run: uv run pytest tests/unit/agent/test_freeze.py
"""

from unittest.mock import AsyncMock

import pytest

import tracker.aws.s3 as s3_module
from tests.utils import TEST_ORG_ID
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import S3ObjectCopy, copy_agent_to_benchmark


class TestCopyAgentToBenchmark:
    """Agent source copies into benchmark workspaces."""

    @pytest.mark.parametrize("destination_exists", [False, True])
    async def test_preserves_frozen_agent_copy(
        self,
        aws_runtime: AWSRuntime,
        monkeypatch: pytest.MonkeyPatch,
        destination_exists: bool,
    ) -> None:
        """Copy an agent once so retries keep using the frozen benchmark version.

        Test cases:
        - A missing destination receives the current agent archive.
        - An existing destination is not overwritten during retry or resume.
        """
        exists_mock = AsyncMock(return_value=destination_exists)
        copy_mock = AsyncMock(return_value="version-1")

        monkeypatch.setattr(s3_module, "s3_object_exists", exists_mock)
        monkeypatch.setattr(s3_module, "copy_s3_object", copy_mock)

        created = await copy_agent_to_benchmark(
            benchmark_id="bench-123",
            contract_name="my_agent",
            runtime=aws_runtime,
            org_id=TEST_ORG_ID,
        )

        assert created == (None if destination_exists else S3ObjectCopy(version_id="version-1"))
        exists_mock.assert_awaited_once_with("benchmarks/bench-123/my_agent.zip", aws_runtime)
        if destination_exists:
            copy_mock.assert_not_awaited()
        else:
            copy_mock.assert_awaited_once_with(
                f"agents/{TEST_ORG_ID}/my_agent.zip",
                "benchmarks/bench-123/my_agent.zip",
                aws_runtime,
            )
