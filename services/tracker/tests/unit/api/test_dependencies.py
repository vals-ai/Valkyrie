"""Run: uv run pytest services/tracker/tests/unit/api/test_dependencies.py"""

from unittest.mock import Mock

from tracker.api.dependencies import get_run_runtime
import pytest
from tracker.database.models import Org
from starlette.requests import Request
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import Benchmark
from tracker.types import HarnessConfig


def test_get_run_runtime_uses_persisted_sandbox_provider(
    example_benchmark_object: Benchmark, harness_config: HarnessConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = AWSRuntime.from_harness_config(harness_config)

    monkeypatch.setattr("tracker.api.dependencies.get_run_aws_context", Mock(return_value=context))
    runtime = get_run_runtime(example_benchmark_object, Request({"type": "http"}), Org(name="test"))
    arguments = example_benchmark_object.arguments
    assert runtime.sandbox_provider == arguments.sandbox_provider
    assert runtime.sandbox_provider_secret_name == arguments.sandbox_provider_secret_name
