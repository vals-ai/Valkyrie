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

EXPECTED_KEYWORD_ONLY = {
    ValkyrieClient: {"base_url", "timeout", "transport"},
    ValkyrieClient.from_config: {"base_url", "timeout", "transport"},
    RunsResource.start: set(EXPECTED_PARAMETERS[RunsResource.start][3:]),
    RunsResource.fetch: set(),
    RunsResource.list: set(),
    RunsResource.stream: set(),
    RunsResource.results: {"task_ids", "upload_to_s3"},
    RunsResource.stop: {"force"},
    RunsResource.resume: {"concurrency", "task_ids", "secrets", "service_headers", "from_scratch"},
    RunsResource.retry: {"concurrency", "task_ids", "secrets", "service_headers", "from_scratch"},
}

EXPECTED_DEFAULTS = {
    ValkyrieClient: {"base_url": None, "timeout": 120, "transport": None},
    ValkyrieClient.from_config: {
        "path": DEFAULT_CONFIG_PATH,
        "base_url": None,
        "timeout": 120,
        "transport": None,
    },
    RunsResource.start: {
        "model": None,
        "concurrency": 5,
        "task_ids": None,
        "slice_str": None,
        "dataset": None,
        "label": None,
        "lambda_function": None,
        "provider": None,
        "agent_kwargs": None,
        "secrets": None,
        "service_headers": None,
        "webhook_intervals": None,
        "ignore_custom_services": False,
    },
    RunsResource.fetch: {},
    RunsResource.list: {"request": None},
    RunsResource.stream: {},
    RunsResource.results: {"task_ids": None, "upload_to_s3": False},
    RunsResource.stop: {"force": False},
    RunsResource.resume: {
        "concurrency": None,
        "task_ids": None,
        "secrets": None,
        "service_headers": None,
        "from_scratch": False,
    },
    RunsResource.retry: {
        "concurrency": None,
        "task_ids": None,
        "secrets": None,
        "service_headers": None,
        "from_scratch": False,
    },
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
    for callable_object, expected_names in EXPECTED_PARAMETERS.items():
        parameters = inspect.signature(callable_object).parameters
        expected_defaults = EXPECTED_DEFAULTS[callable_object]
        keyword_only = EXPECTED_KEYWORD_ONLY[callable_object]
        for name in expected_names:
            expected_kind = (
                inspect.Parameter.KEYWORD_ONLY if name in keyword_only else inspect.Parameter.POSITIONAL_OR_KEYWORD
            )
            assert parameters[name].kind is expected_kind
            if name in expected_defaults:
                assert parameters[name].default == expected_defaults[name]
            else:
                assert parameters[name].default is inspect.Parameter.empty


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


def test_sdk_build_backend_is_reproducibly_pinned() -> None:
    pyproject_path = Path(__file__).parents[3] / "packages" / "valkyrie-sdk" / "pyproject.toml"
    with pyproject_path.open("rb") as pyproject_file:
        pyproject = tomllib.load(pyproject_file)

    assert pyproject["build-system"]["requires"] == ["hatchling==1.27.0"]


def test_root_and_sdk_distributions_support_the_same_python_versions() -> None:
    root = Path(__file__).parents[3]
    with (root / "pyproject.toml").open("rb") as root_file:
        root_project = tomllib.load(root_file)
    with (root / "packages" / "valkyrie-sdk" / "pyproject.toml").open("rb") as sdk_file:
        sdk_project = tomllib.load(sdk_file)

    assert root_project["project"]["requires-python"] == sdk_project["project"]["requires-python"]
