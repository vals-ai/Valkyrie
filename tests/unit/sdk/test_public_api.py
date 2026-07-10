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
    RunsResource.stop: "self, run_id, *, force=False",
    RunsResource.resume: (
        "self, run_id, *, concurrency=None, task_ids=None, secrets=None, service_headers=None, from_scratch=False"
    ),
    RunsResource.retry: (
        "self, run_id, *, concurrency=None, task_ids=None, secrets=None, service_headers=None, from_scratch=False"
    ),
}


def signature_text(callable_object: object) -> str:
    parts: list[str] = []
    keyword_only = False
    for parameter in inspect.signature(callable_object).parameters.values():
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY and not keyword_only:
            parts.append("*")
            keyword_only = True
        part = parameter.name
        if parameter.default is not inspect.Parameter.empty:
            value = str(parameter.default) if isinstance(parameter.default, Path) else repr(parameter.default)
            part += f"={value}"
        parts.append(part)
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


def test_python_support_matches_each_distribution_boundary() -> None:
    root = Path(__file__).parents[3]
    with (root / "pyproject.toml").open("rb") as root_file:
        root_project = tomllib.load(root_file)
    with (root / "packages" / "valkyrie-sdk" / "pyproject.toml").open("rb") as sdk_file:
        sdk_project = tomllib.load(sdk_file)

    assert root_project["project"]["requires-python"] == ">=3.12,<3.13"
    assert sdk_project["project"]["requires-python"] == ">=3.12"
