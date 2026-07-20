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
from valkyrie.sdk.resources import AgentsResource, BenchmarkServicesResource, RunsResource

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
    "FetchTasksRequest",
    "GetRunResponse",
    "ListRunsRequest",
    "ListRunsResponse",
    "Order",
    "RetrieveRunResultsResponse",
    "RetryMode",
    "RetryOrResumeRunResponse",
    "ResultsExistResponse",
    "RunMetadataResponse",
    "RunResultsResponse",
    "RunStatus",
    "RunStatusEntry",
    "RunStatusResponse",
    "S3UploadResultsResponse",
    "SingleTaskResponse",
    "StartRunResponse",
    "StopRunResponse",
    "TaskArtifactsResponse",
    "TasksResponse",
    "TaskSummary",
    "TaskStatus",
    "UpdateRunConcurrencyResponse",
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
    RunsResource.statuses: "self, run_ids",
    RunsResource.tasks: "self, run_id, request=None",
    RunsResource.task: "self, run_id, task_id",
    RunsResource.artifacts: "self, run_id, task_id",
    RunsResource.stream: "self, run_id",
    RunsResource.results: "self, run_id, *, task_ids=None, upload_to_s3=False",
    RunsResource.metadata: "self, run_id",
    RunsResource.results_exist: "self, run_id",
    RunsResource.analyze: "self, run_id, *, no_cache=False, lambda_function=None",
    RunsResource.stream_outputs: "self, run_id, *, task_ids=None",
    RunsResource.stop: "self, run_id, *, force=False, task_ids=None",
    RunsResource.update: "self, run_id, *, concurrency",
    RunsResource.resume: (
        "self, run_id, *, concurrency=None, task_ids=None, secrets=None, service_headers=None, from_scratch=False"
    ),
    RunsResource.retry: (
        "self, run_id, *, concurrency=None, task_ids=None, secrets=None, service_headers=None, from_scratch=False"
    ),
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


def test_unreleased_legacy_run_names_are_not_exported() -> None:
    legacy_run_names = {
        "BenchmarkStatus",
        "BenchmarkStatusEntry",
        "BenchmarkStatusResponse",
        "FetchBenchmarkResponse",
        "FetchBenchmarkMetadataResponse",
        "FetchBenchmarksRequest",
        "FetchBenchmarksResponse",
        "FinalViewResponse",
        "RetryOrResumeBenchmarkResponse",
        "SingleBenchmarkResponse",
        "StartBenchmarkResponse",
        "StopBenchmarkResponse",
    }

    assert legacy_run_names.isdisjoint(sdk.__all__)
    assert all(not hasattr(sdk, name) for name in legacy_run_names)


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
        assert not hasattr(client, "benchmarks")
        for method_name in ("statuses", "tasks", "task", "artifacts"):
            assert hasattr(client.runs, method_name)
        assert isinstance(client.agents, AgentsResource)
        assert isinstance(client.services, BenchmarkServicesResource)
        assert not hasattr(client.services, "check")
