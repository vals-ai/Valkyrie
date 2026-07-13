"""Shared fixtures for Valkyrie SDK unit tests."""

from collections.abc import Callable
from uuid import UUID

import httpx
import pytest

from valkyrie.sdk import ValkyrieClient, ValkyrieConfig

ConfigValuesFactory = Callable[..., dict[str, object]]
SDKConfigFactory = Callable[..., ValkyrieConfig]
FetchResponseFactory = Callable[..., dict[str, object]]
ClientFactory = Callable[..., ValkyrieClient]


@pytest.fixture
def config_values() -> ConfigValuesFactory:
    """Return a factory for complete YAML-shaped SDK configuration."""

    def factory(**overrides: object) -> dict[str, object]:
        values: dict[str, object] = {
            "api_key": "vals-key",
            "AWS_ACCESS_KEY_ID": "aws-key",
            "AWS_SECRET_ACCESS_KEY": "aws-secret",
            "AWS_DEFAULT_REGION": "us-west-2",
            "AWS_SESSION_TOKEN": "aws-session",
            "S3_BUCKET": "runs-bucket",
            "LOG_GROUP": "benchmarks",
            "LOG_RETENTION_POLICY": 30,
            "sandbox_providers": {"modal": "ModalSecret", "daytona": "DaytonaSecret"},
            "default_sandbox_provider": "modal",
            "custom_benchmark_services": {"swebench": "https://local.swebench/"},
            "benchmark_auth": {"swebench": "benchmark-token"},
            "webhook": "SlackWebhook",
        }
        values.update(overrides)
        return values

    return factory


@pytest.fixture
def sdk_config(config_values: ConfigValuesFactory) -> SDKConfigFactory:
    """Return a factory for validated SDK configuration."""

    def factory(**overrides: object) -> ValkyrieConfig:
        return ValkyrieConfig.model_validate(config_values(**overrides))

    return factory


@pytest.fixture
def fetch_response() -> FetchResponseFactory:
    """Return a factory for tracker fetch responses."""

    def factory(run_id: UUID, *, status: str = "IN_PROGRESS") -> dict[str, object]:
        return {
            "benchmark_name": "swebench",
            "benchmark_id": str(run_id),
            "details": {
                "status": status,
                "started_at": "2026-07-08T12:00:00Z",
                "total_tasks": 2,
                "finished_tasks": 0,
                "task_breakdown": {status: 2},
                "docent_reading_status": "IDLE",
                "docent_reading_url": None,
            },
            "s3_bucket_url": "s3://runs-bucket/benchmarks/run",
            "label": "nightly",
            "final_score": None,
        }

    return factory


@pytest.fixture
def make_client(sdk_config: SDKConfigFactory) -> ClientFactory:
    """Return a factory for SDK clients backed by an HTTPX mock transport."""

    def factory(
        handler: Callable[[httpx.Request], httpx.Response],
        *,
        config: ValkyrieConfig | None = None,
    ) -> ValkyrieClient:
        return ValkyrieClient(
            config or sdk_config(),
            base_url="https://tracker.test",
            transport=httpx.MockTransport(handler),
        )

    return factory
