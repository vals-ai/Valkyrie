"""Compatibility tests for the published SDK surface."""

from __future__ import annotations

import inspect
import tomllib
from pathlib import Path

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


def test_sdk_loads_from_workspace_member() -> None:
    sdk_file = Path(sdk.__file__).resolve()
    assert "packages/valkyrie-sdk/src/valkyrie/sdk" in sdk_file.as_posix()


def test_sdk_package_has_local_vcs_ignore_boundary() -> None:
    package_root = Path(__file__).parents[3] / "packages" / "valkyrie-sdk"
    ignore_file = package_root / ".gitignore"

    assert ignore_file.read_text(encoding="utf-8") == ".ruff_cache/\n__pycache__/\ndist/\n"


def test_type_checkers_use_the_supported_python_version() -> None:
    root = Path(__file__).parents[3]
    for pyproject_path in (root / "pyproject.toml", root / "packages" / "valkyrie-sdk" / "pyproject.toml"):
        with pyproject_path.open("rb") as pyproject_file:
            pyproject = tomllib.load(pyproject_file)
        assert pyproject["tool"]["basedpyright"]["pythonVersion"] == "3.12"
