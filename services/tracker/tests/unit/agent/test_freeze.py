"""Unit tests for the per-benchmark agent freeze helper.

Run: uv run pytest tests/unit/agent/test_freeze.py
"""

from unittest.mock import AsyncMock

import pytest

import tracker.aws.s3 as s3_module
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import copy_agent_to_benchmark
from tracker.types import HarnessConfig


class TestCopyAgentToBenchmark:
    """Agent source copies into benchmark workspaces."""

    @pytest.mark.parametrize("destination_exists", [False, True])
    async def test_preserves_frozen_agent_copy(
        self,
        harness_config: HarnessConfig,
        monkeypatch: pytest.MonkeyPatch,
        destination_exists: bool,
    ) -> None:
        """Copy an agent once so retries keep using the frozen benchmark version.

        Test cases:
        - A missing destination receives the current agent archive.
        - An existing destination is not overwritten during retry or resume.
        """
        exists_mock = AsyncMock(return_value=destination_exists)
        copy_mock = AsyncMock()

        # Restore the production key layout replaced by the unit-test S3 fixture.
        def get_contract_s3_key(name: str) -> str:
            return f"agents/{name}.zip"

        monkeypatch.setattr(s3_module, "get_contract_s3_key", get_contract_s3_key)
        monkeypatch.setattr(s3_module, "s3_object_exists", exists_mock)
        monkeypatch.setattr(s3_module, "copy_s3_object", copy_mock)
        aws_runtime = AWSRuntime.from_harness_config(harness_config)

        created = await copy_agent_to_benchmark(
            benchmark_id="bench-123",
            contract_name="my_agent",
            runtime=aws_runtime,
        )

        assert created is not destination_exists
        exists_mock.assert_awaited_once_with("benchmarks/bench-123/my_agent.zip", aws_runtime)
        if destination_exists:
            copy_mock.assert_not_awaited()
        else:
            copy_mock.assert_awaited_once_with(
                "agents/my_agent.zip",
                "benchmarks/bench-123/my_agent.zip",
                aws_runtime,
            )
