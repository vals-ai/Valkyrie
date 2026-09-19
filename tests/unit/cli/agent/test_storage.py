"""Tests for internal run snapshot storage.

Run: uv run pytest tests/unit/cli/agent/test_storage.py
"""

import io
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from tracker.exceptions import S3Error
from valkyrie.sdk.errors import ValkyrieAPIError

from valkyrie.cli.agent import storage


async def test_ingest_reads_current_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("demo/contract.yaml", "ingest_lambda: demo-ingest")
    monkeypatch.setattr(storage.cli_s3, "aws_runtime", object)
    monkeypatch.setattr(storage, "s3_object_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(storage, "download_from_s3", AsyncMock(return_value=buffer.getvalue()))

    assert await storage.get_ingest_lambda_from_s3("demo") == "demo-ingest"

    monkeypatch.setattr(storage, "s3_object_exists", AsyncMock(return_value=False))

    with pytest.raises(S3Error, match="not found"):
        await storage.get_ingest_lambda_from_s3("missing")


async def test_run_start_publish_atomically_refuses_alias_collision(monkeypatch: pytest.MonkeyPatch) -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.agents.push.side_effect = [None, ValkyrieAPIError(409, "Already exists"), ValkyrieAPIError(500, "Failed")]
    monkeypatch.setattr(storage.ValkyrieClient, "from_config", lambda *args, **kwargs: client)

    assert await storage.push_agent_if_absent("demo", Path("/unused")) is True
    assert await storage.push_agent_if_absent("demo", Path("/unused")) is False
    with pytest.raises(ValkyrieAPIError, match="500"):
        await storage.push_agent_if_absent("demo", Path("/unused"))

    client.agents.push.assert_awaited_with(Path("/unused"), name="demo", overwrite=False)
