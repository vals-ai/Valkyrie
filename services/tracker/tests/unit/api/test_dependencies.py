"""Run: uv run pytest services/tracker/tests/unit/api/test_dependencies.py"""

from tracker.api.dependencies import RunAWSContext, get_run_runtime
from tracker.aws.runtime import AWSRuntime
from tracker.database.models import Benchmark
from tracker.types import HarnessConfig


async def test_get_run_runtime_uses_persisted_sandbox_provider(
    example_benchmark_object: Benchmark, harness_config: HarnessConfig
) -> None:
    context = RunAWSContext(
        benchmark=example_benchmark_object, aws_runtime=AWSRuntime.from_harness_config(harness_config)
    )

    runtime = await get_run_runtime(context)
    arguments = example_benchmark_object.arguments
    assert runtime.sandbox_provider == arguments.sandbox_provider
    assert runtime.sandbox_provider_secret_name == arguments.sandbox_provider_secret_name
