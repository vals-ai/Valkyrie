"""Local service composition uses persistent files and transient credentials."""

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest

from tracker.exceptions import SecretsError
from tracker.local.runtime import LocalRuntimeConfig, LocalRuntimeFactory


async def test_local_runtime_scopes_files_and_clears_secrets(tmp_path: Path) -> None:
    """Persist artifacts without allowing another organization to read them."""
    config = LocalRuntimeConfig(data_root=tmp_path, host_data_root=Path("/host/valkyrie"))
    org_id = uuid4()
    references = {"API_KEY": "agent-key"}
    async with LocalRuntimeFactory.open(
        config, org_id, secret_references=references, execution_secrets={"API_KEY": "transient-value"}
    ) as runtime:
        await runtime.objects.put_bytes("agent.zip", b"agent")
        assert await runtime.resolve_secrets(references) == {"API_KEY": "transient-value"}
        assert runtime.artifacts.object_location("agent.zip") == f"/host/valkyrie/orgs/{org_id}/objects/agent.zip"
        assert (await runtime.get_sandbox_provider_config()).type == "docker"
    with pytest.raises(SecretsError, match="no values"):
        await runtime.resolve_secrets(references)
    async with LocalRuntimeFactory.open(config, org_id) as reopened:
        assert await reopened.objects.get_bytes("agent.zip") == b"agent"
    async with LocalRuntimeFactory.open(config, uuid4()) as other:
        assert not await other.objects.exists("agent.zip")
    assert all(b"transient-value" not in path.read_bytes() for path in tmp_path.rglob("*") if path.is_file())


async def test_local_runtime_discards_secrets_on_cancellation(tmp_path: Path) -> None:
    """Release operation credentials when an executing request is cancelled."""
    config = LocalRuntimeConfig(data_root=tmp_path, host_data_root=tmp_path)
    async with LocalRuntimeFactory.open(
        config, uuid4(), secret_references={"KEY": "secret"}, execution_secrets={"KEY": "value"}
    ) as runtime:
        store = runtime.secrets
    with pytest.raises(SecretsError):
        store.get("secret")

    with pytest.raises(asyncio.CancelledError):
        async with LocalRuntimeFactory.open(
            config, uuid4(), secret_references={"KEY": "secret"}, execution_secrets={"KEY": "value"}
        ) as cancelled:
            store = cancelled.secrets
            raise asyncio.CancelledError
    with pytest.raises(SecretsError):
        store.get("secret")
