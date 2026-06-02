"""Integration tests for pinned create-benchmark-service sandbox provider behavior."""

import os
import uuid
from collections.abc import AsyncGenerator

import pytest
from benchmark_service import ImageSource, Resources, Sandbox, SandboxCreateRequest
from benchmark_service.sandbox import DaytonaBackendConfig
from benchmark_service.sandbox.daytona import DaytonaSandboxProvider

from tracker.types import AWSCredentials, HarnessConfig


@pytest.fixture
def harness_config() -> HarnessConfig:
    """Provide app dependency config while this integration test uses direct Daytona credentials."""
    return HarnessConfig(
        aws=AWSCredentials(
            aws_access_key_id="test-access-key",
            aws_secret_access_key="test-secret-key",
            aws_default_region="us-east-1",
        ),
        s3_bucket="test-bucket",
        log_group="test-log-group",
        log_retention_policy=1,
        daytona_secret_name="test-daytona-secret",
    )


@pytest.fixture
async def sandbox_provider() -> AsyncGenerator[DaytonaSandboxProvider, None]:
    """
    Create a Daytona sandbox provider from real API credentials.

    Test cases:
    - Daytona API credentials are read from the local integration-test environment.
    - The provider is closed after the real API test completes.
    """
    api_key = os.getenv("DAYTONA_API_KEY")
    api_url = os.getenv("DAYTONA_API_URL")
    target = os.getenv("DAYTONA_TARGET")
    if not api_key or not api_url or not target:
        raise ValueError("DAYTONA_API_KEY, DAYTONA_API_URL, and DAYTONA_TARGET are required")

    provider = DaytonaSandboxProvider(DaytonaBackendConfig(api_key=api_key, api_url=api_url, target=target))
    try:
        yield provider
    finally:
        await provider.close()


def _request(name: str, source: ImageSource, resources: Resources) -> SandboxCreateRequest:
    return SandboxCreateRequest(
        source=source,
        resources=resources,
        name=name,
        labels={},
        env_vars={},
        auto_stop_interval=600,
        create_timeout=360,
    )


async def test_daytona_provider_dependency_recreates_name_after_delete(
    sandbox_provider: DaytonaSandboxProvider,
) -> None:
    """
    Tracker depends on create-benchmark-service to wait when Daytona keeps a deleted sandbox name reserved.

    Test cases:
    - A real Daytona sandbox can be deleted and recreated immediately with the same name.
    - The recreated sandbox is usable and is cleaned up through the provider API.
    """
    sandbox_name = f"test-recreate-{uuid.uuid4().hex[:8]}"
    source = ImageSource(image="python:3.11-slim")
    resources = Resources(vcpu=1, memory=2, disk=5)
    request = _request(sandbox_name, source, resources)
    recreated_sandbox: Sandbox | None = None

    first_sandbox = await sandbox_provider.create_sandbox(request)
    await sandbox_provider.delete_sandbox(first_sandbox.id)

    try:
        recreated_sandbox = await sandbox_provider.create_sandbox(request)
        result = await recreated_sandbox.exec("echo recreated")

        assert recreated_sandbox.name == sandbox_name
        assert result.exit_code == 0
        assert result.output.strip() == "recreated"
    finally:
        if recreated_sandbox is not None:
            await sandbox_provider.delete_sandbox(recreated_sandbox.id)
