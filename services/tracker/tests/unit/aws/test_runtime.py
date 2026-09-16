"""Runtime composition and sandbox lifetime behavior."""

import asyncio
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from tracker.aws.clients import AWSClientProvider
from tracker.aws.runtime import AWSResources
from tracker.aws.services import AccessKeyRuntimeConfig
from tracker.exceptions import InvalidSandboxConfigurationError
from tracker.runtime import services
from tracker.aws.secrets import SecretsManagerStore


@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_runtime_reuses_and_closes_provider(
    monkeypatch: pytest.MonkeyPatch,
    failure: type[BaseException] | None,
) -> None:
    """Concurrent access creates one provider and cleanup survives execution failure."""
    provider = MagicMock(close=AsyncMock())
    provider_config = MagicMock()
    provider_config.create_provider.return_value = provider
    resolve = AsyncMock(return_value={"api_key": "test-provider-key"})
    monkeypatch.setattr(SecretsManagerStore, "get_async", resolve)
    monkeypatch.setattr(services, "sandbox_provider_config_from_mapping", MagicMock(return_value=provider_config))
    clients = MagicMock(spec=AWSClientProvider)
    config = AccessKeyRuntimeConfig(properties=AWSResources("us-east-1", "bucket", "logs", 30))

    async def execute() -> None:
        async with config.create_runtime(
            clients=cast(AWSClientProvider, clients),
            sandbox_provider="modal",
            sandbox_provider_secret_name="provider-reference",
        ) as runtime:
            resolve.assert_not_awaited()
            providers = await asyncio.gather(runtime.get_sandbox_provider(), runtime.get_sandbox_provider())
            assert providers == [provider, provider]
            resolve.assert_awaited_once_with("provider-reference")
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
    clients = MagicMock(spec=AWSClientProvider)
    config = AccessKeyRuntimeConfig(properties=AWSResources("us-east-1", "bucket", "logs", 30))
    async with config.create_runtime(clients=cast(AWSClientProvider, clients)) as runtime:
        assert "bucket" in runtime.artifacts.object_location("result.json")
        with pytest.raises(InvalidSandboxConfigurationError, match="provider secret name"):
            await runtime.get_sandbox_provider()
    assert clients.mock_calls == []


async def test_shutdown_waits_for_loading_and_rejects_late_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    """Closing during secret lookup must not create a provider after shutdown."""
    started, release = asyncio.Event(), asyncio.Event()
    provider_config = MagicMock()

    async def load(*_args: object) -> object:
        started.set()
        await release.wait()
        return {"api_key": "test-provider-key"}

    monkeypatch.setattr(SecretsManagerStore, "get_async", load)
    monkeypatch.setattr(services, "sandbox_provider_config_from_mapping", MagicMock(return_value=provider_config))
    config = AccessKeyRuntimeConfig(properties=AWSResources("us-east-1", "bucket", "logs", 30))
    async with config.create_runtime(clients=MagicMock(), sandbox_provider_secret_name="provider") as runtime:
        loading = asyncio.create_task(runtime.get_sandbox_provider())
        await started.wait()
        closing = asyncio.create_task(runtime.close())
        await asyncio.sleep(0)
        assert not closing.done()

        release.set()
        with pytest.raises(services.TrackerServiceError, match="closed"):
            await loading
        await closing

    provider_config.create_provider.assert_not_called()


async def test_cancelled_shutdown_drains_provider(monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr(SecretsManagerStore, "get_async", AsyncMock(return_value={"api_key": "test-provider-key"}))
    monkeypatch.setattr(services, "sandbox_provider_config_from_mapping", MagicMock(return_value=provider_config))
    config = AccessKeyRuntimeConfig(properties=AWSResources("us-east-1", "bucket", "logs", 30))
    async with config.create_runtime(clients=MagicMock(), sandbox_provider_secret_name="provider") as runtime:
        await runtime.get_sandbox_provider()
        closing = asyncio.create_task(runtime.close())
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
