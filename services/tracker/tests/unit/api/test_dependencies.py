"""Run: uv run pytest services/tracker/tests/unit/api/test_dependencies.py"""

from tracker.api.dependencies import RunAWSContext, get_run_runtime
from unittest.mock import Mock
import pytest
from tracker.database.models import Org
from starlette.requests import Request
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import Benchmark
from tracker.types import HarnessConfig


async def test_get_run_runtime_uses_persisted_sandbox_provider(
    example_benchmark_object: Benchmark, harness_config: HarnessConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = RunAWSContext(
        benchmark=example_benchmark_object, aws_runtime=AWSRuntime.from_harness_config(harness_config)
    )

    monkeypatch.setattr("tracker.api.dependencies.get_scoped", lambda *args: example_benchmark_object)
    monkeypatch.setattr("tracker.api.dependencies.get_run_aws_context", lambda *args: context)
    runtime = await get_run_runtime(example_benchmark_object.id, Request({"type": "http"}), Mock(), Org())
    arguments = example_benchmark_object.arguments
    assert runtime.sandbox_provider == arguments.sandbox_provider
    assert runtime.sandbox_provider_secret_name == arguments.sandbox_provider_secret_name
