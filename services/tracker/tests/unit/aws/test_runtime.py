"""Runtime composition and sandbox lifetime behavior."""

import asyncio
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from tracker.aws.clients import AWSClientProvider
from tracker.aws.runtime import AWSResources, CloudRuntimeConfig
from tracker.exceptions import InvalidSandboxConfigurationError
from tracker.utils import resources


@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_runtime_reuses_and_closes_provider(
    monkeypatch: pytest.MonkeyPatch,
    failure: type[BaseException] | None,
) -> None:
    """Concurrent access creates one provider and cleanup survives execution failure."""
    provider = MagicMock(close=AsyncMock())
    provider_config = MagicMock()
    provider_config.create_provider.return_value = provider
    resolve = AsyncMock(return_value=provider_config)
    monkeypatch.setattr(resources, "fetch_sandbox_provider_config_async", resolve)
    clients = MagicMock(spec=AWSClientProvider)
    config = CloudRuntimeConfig(properties=AWSResources("us-east-1", "bucket", "logs", 30))

    async def execute() -> None:
        async with config.create_runtime(
            clients=cast(AWSClientProvider, clients),
            sandbox_provider="modal",
            sandbox_provider_secret_name="provider-reference",
        ) as runtime:
            resolve.assert_not_awaited()
            providers = await asyncio.gather(runtime.get_sandbox_provider(), runtime.get_sandbox_provider())
            assert providers == [provider, provider]
            resolve.assert_awaited_once_with("provider-reference", runtime.async_secrets, "modal")
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
    config = CloudRuntimeConfig(properties=AWSResources("us-east-1", "bucket", "logs", 30))
    async with config.create_runtime(clients=cast(AWSClientProvider, clients)) as runtime:
        assert "bucket" in runtime.artifacts.object_location("result.json")
        with pytest.raises(InvalidSandboxConfigurationError, match="provider secret name"):
            await runtime.get_sandbox_provider()
    assert clients.mock_calls == []
