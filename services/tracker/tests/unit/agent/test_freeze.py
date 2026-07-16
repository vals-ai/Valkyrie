"""Unit tests for the per-benchmark agent freeze helper.

Run: uv run pytest tests/unit/agent/test_freeze.py
"""

from unittest.mock import AsyncMock

from pytest import MonkeyPatch

import tracker.aws.s3 as s3_module
from tracker.aws.s3 import copy_agent_to_benchmark
from tracker.types import HarnessConfig


class TestCopyAgentToBenchmark:
    """Agent source copies into benchmark workspaces."""

    async def test_copies_when_destination_missing(
        self, harness_config: HarnessConfig, monkeypatch: MonkeyPatch
    ) -> None:
        """On first call, the agent zip is copied from agents/<name>.zip into the benchmark folder."""
        exists_mock = AsyncMock(return_value=False)
        copy_mock = AsyncMock()

        # The unit-test autouse mock_s3 fixture patches get_contract_s3_key to return
        # "contracts/<name>.zip"; restore the real key layout so we assert the true invariant.
        def get_contract_s3_key(name: str) -> str:
            return f"agents/{name}.zip"

        monkeypatch.setattr(s3_module, "get_contract_s3_key", get_contract_s3_key)
        monkeypatch.setattr(s3_module, "s3_object_exists", exists_mock)
        monkeypatch.setattr(s3_module, "copy_s3_object", copy_mock)

        await copy_agent_to_benchmark(
            benchmark_id="bench-123",
            contract_name="my_agent",
            aws=harness_config.aws,
            s3_bucket="test-bucket",
        )

        exists_mock.assert_awaited_once_with("benchmarks/bench-123/my_agent.zip", harness_config.aws, "test-bucket")
        copy_mock.assert_awaited_once_with(
            "agents/my_agent.zip",
            "benchmarks/bench-123/my_agent.zip",
            harness_config.aws,
            "test-bucket",
        )

    async def test_skips_copy_when_destination_exists(
        self, harness_config: HarnessConfig, monkeypatch: MonkeyPatch
    ) -> None:
        """Retry/resume must not overwrite the frozen agent copy. If benchmarks/<id>/<name>.zip
        already exists, copy_agent_to_benchmark must not call copy_s3_object.
        """
        exists_mock = AsyncMock(return_value=True)
        copy_mock = AsyncMock()

        def get_contract_s3_key(name: str) -> str:
            return f"agents/{name}.zip"

        monkeypatch.setattr(s3_module, "get_contract_s3_key", get_contract_s3_key)
        monkeypatch.setattr(s3_module, "s3_object_exists", exists_mock)
        monkeypatch.setattr(s3_module, "copy_s3_object", copy_mock)

        await copy_agent_to_benchmark(
            benchmark_id="bench-123",
            contract_name="my_agent",
            aws=harness_config.aws,
            s3_bucket="test-bucket",
        )

        exists_mock.assert_awaited_once_with("benchmarks/bench-123/my_agent.zip", harness_config.aws, "test-bucket")
        copy_mock.assert_not_awaited()
