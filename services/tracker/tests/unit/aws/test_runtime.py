"""Runtime composition and lazy sandbox configuration."""

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


async def test_read_only_runtime_needs_no_provider_credentials() -> None:
    """Artifact locations work without touching AWS or sandbox credentials."""
    clients = MagicMock(spec=AWSClientProvider, credential_source="access_key")
    aws_runtime = AWSRuntime(AWSResources("us-east-1", "bucket", "logs", 30), cast(AWSClientProvider, clients))
    runtime = CloudRuntimeFactory.create_runtime(aws_runtime)
    assert "bucket" in runtime.artifacts.object_location("result.json")
    with pytest.raises(InvalidSandboxConfigurationError, match="provider secret name"):
        await runtime.get_sandbox_provider_config()
    assert clients.mock_calls == []
