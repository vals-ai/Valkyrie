"""Tests for internal run snapshot storage.

Run: uv run pytest tests/unit/cli/agent/test_storage.py
"""

from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from valkyrie.sdk.errors import ValkyrieAPIError

from valkyrie.cli.agent import storage


@pytest.mark.parametrize("extension", ["yaml", "yml"])
async def test_ingest_reads_current_contract(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, extension: str) -> None:
    agent_path = tmp_path / "demo"
    agent_path.mkdir()
    contract_path = agent_path / f"contract.{extension}"
    contract_path.write_text("ingest_lambda: demo-ingest")
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.agents.download.return_value = agent_path
    monkeypatch.setattr(storage.ValkyrieClient, "from_config", Mock(return_value=client))

    assert await storage.get_ingest_lambda("demo") == "demo-ingest"

    contract_path.write_text("name: demo")

    assert await storage.get_ingest_lambda("demo") is None

    client.agents.download.side_effect = ValkyrieAPIError(404, "Agent not found")

    with pytest.raises(ValkyrieAPIError, match="404"):
        await storage.get_ingest_lambda("missing")


async def test_run_start_publish_atomically_refuses_alias_collision(monkeypatch: pytest.MonkeyPatch) -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.agents.push.side_effect = [None, ValkyrieAPIError(409, "Already exists"), ValkyrieAPIError(500, "Failed")]
    monkeypatch.setattr(storage.ValkyrieClient, "from_config", Mock(return_value=client))

    assert await storage.push_agent_if_absent("demo", Path("/unused")) is True
    assert await storage.push_agent_if_absent("demo", Path("/unused")) is False
    with pytest.raises(ValkyrieAPIError, match="500"):
        await storage.push_agent_if_absent("demo", Path("/unused"))

    client.agents.push.assert_awaited_with(Path("/unused"), name="demo", overwrite=False)
