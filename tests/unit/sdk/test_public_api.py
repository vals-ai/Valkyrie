"""Published SDK compatibility tests.

Run: pytest tests/unit/sdk/test_public_api.py
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from pathlib import Path

import valkyrie.sdk as sdk
from valkyrie.sdk.client import DEFAULT_BASE_URL, ValkyrieClient
from valkyrie.sdk.config import DEFAULT_CONFIG_PATH, ValkyrieConfig
from valkyrie.sdk.resources import AgentsResource, BenchmarksResource, BenchmarkServicesResource, RunsResource

EXPECTED_ALL = [
    "AgentContractRequest",
    "AgentDownloadURLResponse",
    "AgentEntry",
    "AgentsResponse",
    "AnalyzeEvent",
    "BenchmarkServiceCatalogResponse",
    "BenchmarkServiceEntry",
    "BenchmarkServiceHealth",
    "BenchmarkServicesResponse",
    "BenchmarkStatus",
    "BenchmarkStatusEntry",
    "BenchmarkStatusResponse",
    "FetchBenchmarkResponse",
    "FetchBenchmarkMetadataResponse",
    "FetchBenchmarksRequest",
    "FetchBenchmarksResponse",
    "FetchTasksRequest",
    "FinalViewResponse",
    "Order",
    "RetryMode",
    "RetryOrResumeBenchmarkResponse",
    "ResultsExistResponse",
    "S3UploadResultsResponse",
    "SingleBenchmarkResponse",
    "SingleTaskResponse",
    "StartBenchmarkResponse",
    "StopBenchmarkResponse",
    "TaskArtifactsResponse",
    "TasksResponse",
    "TaskSummary",
    "TaskStatus",
    "ValkyrieAPIError",
    "ValkyrieClient",
    "ValkyrieConfig",
    "ValkyrieConfigError",
    "ValkyrieRunError",
    "ValkyrieSDKError",
    "ValkyrieStreamError",
    "ValkyrieTransportError",
]

EXPECTED_SIGNATURES = {
    ValkyrieClient: "config, *, base_url=None, timeout=120, transport=None",
    ValkyrieClient.from_config: (
        "path=~/.config/valkyrie/valkyrie.yaml, *, base_url=None, timeout=120, transport=None"
    ),
    RunsResource.start: (
        "self, agent, benchmark, *, model=None, concurrency=5, task_ids=None, slice_str=None, dataset=None, "
        "label=None, lambda_function=None, provider=None, agent_kwargs=None, secrets=None, service_headers=None, "
        "webhook_intervals=None, ignore_custom_services=False"
    ),
    RunsResource.fetch: "self, run_id",
    RunsResource.list: "self, request=None",
    RunsResource.stream: "self, run_id",
    RunsResource.results: "self, run_id, *, task_ids=None, upload_to_s3=False",
    RunsResource.metadata: "self, run_id",
    RunsResource.results_exist: "self, run_id",
    RunsResource.analyze: "self, run_id, *, no_cache=False, lambda_function=None",
    RunsResource.stream_outputs: "self, run_id, *, task_ids=None",
    RunsResource.stop: "self, run_id, *, force=False, task_ids=None",
    RunsResource.resume: (
        "self, run_id, *, concurrency=None, task_ids=None, secrets=None, service_headers=None, from_scratch=False, "
        "benchmark_url=None"
    ),
    RunsResource.retry: (
        "self, run_id, *, concurrency=None, task_ids=None, secrets=None, service_headers=None, from_scratch=False, "
        "benchmark_url=None"
    ),
    BenchmarksResource.fetch: "self, run_id",
    BenchmarksResource.statuses: "self, run_ids",
    BenchmarksResource.tasks: "self, run_id, request=None",
    BenchmarksResource.task: "self, run_id, task_id",
    BenchmarksResource.artifacts: "self, run_id, task_id",
    AgentsResource.list: "self",
    AgentsResource.download_url: "self, name",
    BenchmarkServicesResource.catalog: "self",
    BenchmarkServicesResource.list: "self",
    BenchmarkServicesResource.task_ids: (
        "self, benchmark, *, dataset=None, service_headers=None, ignore_custom_services=False"
    ),
}


def signature_text(callable_object: Callable[..., object]) -> str:
    parts: list[str] = []
    keyword_only = False
    parameters = list(inspect.signature(callable_object).parameters.values())
    for index, parameter in enumerate(parameters):
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY and not keyword_only:
            parts.append("*")
            keyword_only = True
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            part = f"*{parameter.name}"
            keyword_only = True
        elif parameter.kind is inspect.Parameter.VAR_KEYWORD:
            part = f"**{parameter.name}"
        else:
            part = parameter.name
        if parameter.default is not inspect.Parameter.empty:
            value = str(parameter.default) if isinstance(parameter.default, Path) else repr(parameter.default)
            part += f"={value}"
        parts.append(part)
        if parameter.kind is inspect.Parameter.POSITIONAL_ONLY and (
            index + 1 == len(parameters) or parameters[index + 1].kind is not inspect.Parameter.POSITIONAL_ONLY
        ):
            parts.append("/")
    return ", ".join(parts)


def test_public_exports_and_constants_are_stable() -> None:
    assert sdk.__all__ == EXPECTED_ALL
    assert DEFAULT_BASE_URL == "https://benchmark-tracker.vals.ai"
    assert str(DEFAULT_CONFIG_PATH) == "~/.config/valkyrie/valkyrie.yaml"
    assert sdk.ValkyrieClient is ValkyrieClient
    assert sdk.ValkyrieConfig is ValkyrieConfig


def test_public_signatures_are_stable() -> None:
    for callable_object, expected in EXPECTED_SIGNATURES.items():
        assert signature_text(callable_object) == expected


def test_signature_text_preserves_parameter_kinds() -> None:
    def sample(value: object, /, other: object, *, flag: bool = False) -> None:
        pass

    assert signature_text(sample) == "value, /, other, *, flag=False"


async def test_client_exposes_v2_resource_namespaces(make_client) -> None:
    async with make_client(lambda _request: None) as client:
        assert isinstance(client.runs, RunsResource)
        assert isinstance(client.benchmarks, BenchmarksResource)
        assert isinstance(client.agents, AgentsResource)
        assert isinstance(client.services, BenchmarkServicesResource)
        assert not hasattr(client.services, "check")
