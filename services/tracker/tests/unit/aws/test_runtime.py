"""Runtime composition and lazy sandbox configuration."""

import asyncio
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from benchmark_service.sandbox.modal import ModalProviderConfig

from tracker.aws.clients import AWSClientProvider
from tracker.aws.runtime import AWSResources, AWSRuntime
from tracker.aws.services import CloudRuntimeFactory
from tracker.exceptions import InvalidSandboxConfigurationError
from tracker.aws.secrets import SecretsManagerStore


async def test_runtime_resolves_provider_configuration_when_requested(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
) -> None:
    """Read the persisted secret only when sandbox configuration is needed."""
    resolve = AsyncMock(return_value={"MODAL_TOKEN_ID": "id", "MODAL_TOKEN_SECRET": "secret"})
    monkeypatch.setattr(SecretsManagerStore, "get", resolve)
    runtime = CloudRuntimeFactory.create_runtime(
        aws_runtime,
        sandbox_provider="modal",
        sandbox_provider_secret_name="provider-reference",
    )
    resolve.assert_not_awaited()

    config = await runtime.get_sandbox_provider_config()

    resolve.assert_awaited_once_with("provider-reference")
    assert isinstance(config, ModalProviderConfig)
    assert config.MODAL_TOKEN_ID == "id"
    assert config.MODAL_TOKEN_SECRET == "secret"


@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_runtime_reuses_and_closes_provider(
    monkeypatch: pytest.MonkeyPatch,
    failure: type[BaseException] | None,
) -> None:
    """One execution shares its provider and closes it on success, failure, or cancellation."""
    provider = MagicMock(close=AsyncMock())
    provider_config = MagicMock()
    provider_config.create_provider.return_value = provider
    resolve = AsyncMock(return_value={"api_key": "test-provider-key"})
    monkeypatch.setattr(SecretsManagerStore, "get", resolve)
    monkeypatch.setattr(
        "tracker.runtime.secrets.sandbox_provider_config_from_mapping", MagicMock(return_value=provider_config)
    )
    clients = MagicMock(spec=AWSClientProvider, credential_source="access_key")
    aws_runtime = AWSRuntime(AWSResources("us-east-1", "bucket", "logs", 30), cast(AWSClientProvider, clients))

    async def execute() -> None:
        runtime = CloudRuntimeFactory.create_runtime(
            aws_runtime,
            sandbox_provider="modal",
            sandbox_provider_secret_name="provider-reference",
        )
        resolve.assert_not_awaited()
        config = await runtime.get_sandbox_provider_config()
        async with runtime.get_sandbox_provider(config) as sandbox_provider:
            assert sandbox_provider is provider
            resolve.assert_awaited_once_with("provider-reference")
            provider.close.assert_not_awaited()
            if failure is not None:
                raise failure()

    if failure is None:
        await execute()
    else:
        with pytest.raises(failure):
            await execute()
    provider_config.create_provider.assert_called_once_with()
    provider.close.assert_awaited_once_with()


async def test_read_only_runtime_needs_no_provider_credentials() -> None:
    """Artifact locations work without touching AWS or sandbox credentials."""
    clients = MagicMock(spec=AWSClientProvider, credential_source="access_key")
    aws_runtime = AWSRuntime(AWSResources("us-east-1", "bucket", "logs", 30), cast(AWSClientProvider, clients))
    runtime = CloudRuntimeFactory.create_runtime(aws_runtime)
    assert "bucket" in runtime.artifacts.object_location("result.json")
    with pytest.raises(InvalidSandboxConfigurationError, match="provider secret name"):
        await runtime.get_sandbox_provider_config()
    assert clients.mock_calls == []


def test_runtime_resource_replacement_preserves_expected_bucket_owner() -> None:
    clients = MagicMock(spec=AWSClientProvider, credential_source="managed")
    regional_clients = MagicMock(spec=AWSClientProvider, credential_source="managed")
    clients.with_region.return_value = regional_clients
    runtime = AWSRuntime(
        AWSResources("us-east-1", "original-bucket", "logs", 30),
        cast(AWSClientProvider, clients),
        expected_bucket_owner="123456789012",
    )
    resources = AWSResources("us-west-2", "saved-bucket", "saved-logs", 14)

    replaced = runtime.with_resources(resources)

    assert replaced.resources == resources
    assert replaced.clients is regional_clients
    assert replaced.expected_bucket_owner == "123456789012"


async def test_cancelled_shutdown_drains_provider(monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime) -> None:
    """Repeated caller cancellation does not interrupt provider shutdown."""
    started, release = asyncio.Event(), asyncio.Event()
    finished = asyncio.Event()

    async def close() -> None:
        started.set()
        await release.wait()
        finished.set()

    provider = MagicMock(close=AsyncMock(side_effect=close))
    provider_config = MagicMock()
    provider_config.create_provider.return_value = provider
    monkeypatch.setattr(SecretsManagerStore, "get", AsyncMock(return_value={"api_key": "test-provider-key"}))
    monkeypatch.setattr(
        "tracker.runtime.secrets.sandbox_provider_config_from_mapping", MagicMock(return_value=provider_config)
    )

    async def execute() -> None:
        runtime = CloudRuntimeFactory.create_runtime(aws_runtime, sandbox_provider_secret_name="provider")
        config = await runtime.get_sandbox_provider_config()
        async with runtime.get_sandbox_provider(config):
            pass

    closing = asyncio.create_task(execute())
    await started.wait()
    for _ in range(2):
        closing.cancel()
        await asyncio.sleep(0)
        assert not closing.done()

    release.set()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert finished.is_set()
    provider.close.assert_awaited_once()
