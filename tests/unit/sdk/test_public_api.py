"""Compatibility tests for the published SDK surface."""

from __future__ import annotations

import inspect

import valkyrie.sdk as sdk
from valkyrie.sdk.client import DEFAULT_BASE_URL, ValkyrieClient
from valkyrie.sdk.config import DEFAULT_CONFIG_PATH, ValkyrieConfig
from valkyrie.sdk.resources import RunsResource

EXPECTED_ALL = [
    "AgentContractRequest",
    "BenchmarkStatus",
    "FetchBenchmarkResponse",
    "FetchBenchmarksRequest",
    "FetchBenchmarksResponse",
    "FinalViewResponse",
    "RetryMode",
    "RetryOrResumeBenchmarkResponse",
    "S3UploadResultsResponse",
    "StartBenchmarkResponse",
    "StopBenchmarkResponse",
    "ValkyrieAPIError",
    "ValkyrieClient",
    "ValkyrieConfig",
    "ValkyrieConfigError",
    "ValkyrieRunError",
    "ValkyrieSDKError",
    "ValkyrieStreamError",
    "ValkyrieTransportError",
]

EXPECTED_PARAMETERS = {
    ValkyrieClient: ("config", "base_url", "timeout", "transport"),
    ValkyrieClient.from_config: ("path", "base_url", "timeout", "transport"),
    RunsResource.start: (
        "self",
        "agent",
        "benchmark",
        "model",
        "concurrency",
        "task_ids",
        "slice_str",
        "dataset",
        "label",
        "lambda_function",
        "provider",
        "agent_kwargs",
        "secrets",
        "service_headers",
        "webhook_intervals",
        "ignore_custom_services",
    ),
    RunsResource.fetch: ("self", "run_id"),
    RunsResource.list: ("self", "request"),
    RunsResource.stream: ("self", "run_id"),
    RunsResource.results: ("self", "run_id", "task_ids", "upload_to_s3"),
    RunsResource.stop: ("self", "run_id", "force"),
    RunsResource.resume: (
        "self",
        "run_id",
        "concurrency",
        "task_ids",
        "secrets",
        "service_headers",
        "from_scratch",
    ),
    RunsResource.retry: (
        "self",
        "run_id",
        "concurrency",
        "task_ids",
        "secrets",
        "service_headers",
        "from_scratch",
    ),
}


def test_public_exports_and_constants_are_stable() -> None:
    assert sdk.__all__ == EXPECTED_ALL
    assert DEFAULT_BASE_URL == "https://benchmark-tracker.vals.ai"
    assert str(DEFAULT_CONFIG_PATH) == "~/.config/valkyrie/valkyrie.yaml"
    assert sdk.ValkyrieClient is ValkyrieClient
    assert sdk.ValkyrieConfig is ValkyrieConfig


def test_public_callable_parameter_order_is_stable() -> None:
    for callable_object, expected in EXPECTED_PARAMETERS.items():
        assert tuple(inspect.signature(callable_object).parameters) == expected


def test_public_keyword_only_defaults_are_stable() -> None:
    start = inspect.signature(RunsResource.start).parameters
    assert start["concurrency"].default == 5
    assert start["ignore_custom_services"].default is False
    assert start["model"].kind is inspect.Parameter.KEYWORD_ONLY
    results = inspect.signature(RunsResource.results).parameters
    assert results["upload_to_s3"].default is False
    stop = inspect.signature(RunsResource.stop).parameters
    assert stop["force"].default is False
