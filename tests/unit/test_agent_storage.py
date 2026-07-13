import io
import zipfile
from pathlib import Path

import pytest
from tracker.exceptions import S3Error
from tracker.types import AWSCredentials

from valkyrie.cli.agent import storage


def _agent_zip(agent_name: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zip_file:
        zip_file.writestr(
            f"{agent_name}/contract.yaml",
            "name: demo\ninstall_cmd: 'true'\nrun_cmd: 'true'\ningest_lambda: demo-ingest\n",
        )
    return buffer.getvalue()


@pytest.mark.asyncio
async def test_agent_download_ingest_and_remove_use_configured_s3(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Agent storage should preserve download, ingest lookup, remove, and missing-agent behavior.

    Test cases:
    - Existing agent downloads, extracts, reads ingest metadata, and deletes from the configured bucket.
    - Missing agent raises the user-facing S3Error without mutating downloaded or deleted keys.
    """
    credentials = AWSCredentials(
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        aws_default_region="us-east-1",
    )
    existing_keys = {"agents/demo.zip"}
    downloaded_keys: list[str] = []
    deleted_keys: list[str] = []

    async def s3_object_exists(key: str, aws: AWSCredentials, bucket_name: str) -> bool:
        assert aws == credentials
        assert bucket_name == "bucket"
        return key in existing_keys

    async def download_from_s3(key: str, aws: AWSCredentials, bucket_name: str) -> bytes:
        assert aws == credentials
        assert bucket_name == "bucket"
        downloaded_keys.append(key)
        return _agent_zip("demo")

    async def delete_from_s3(key: str, aws: AWSCredentials, bucket_name: str) -> None:
        assert aws == credentials
        assert bucket_name == "bucket"
        deleted_keys.append(key)
        existing_keys.remove(key)

    monkeypatch.setattr(storage, "aws_credentials", lambda: credentials)
    monkeypatch.setattr(storage, "fetch_bucket_name", lambda: "bucket")
    monkeypatch.setattr(storage, "s3_object_exists", s3_object_exists)
    monkeypatch.setattr(storage, "download_from_s3", download_from_s3)
    monkeypatch.setattr(storage, "delete_from_s3", delete_from_s3)

    await storage.download_agent("demo", tmp_path)
    assert (tmp_path / "demo" / "contract.yaml").exists()
    assert await storage.get_ingest_lambda_from_s3("demo") == "demo-ingest"
    await storage.remove_agent("demo")

    assert downloaded_keys == ["agents/demo.zip", "agents/demo.zip"]
    assert deleted_keys == ["agents/demo.zip"]
    with pytest.raises(S3Error, match="Agent 'missing' not found in S3"):
        await storage.download_agent("missing", tmp_path)
    with pytest.raises(S3Error, match="Agent 'missing' could not be found"):
        await storage.remove_agent("missing")
    assert downloaded_keys == ["agents/demo.zip", "agents/demo.zip"]
    assert deleted_keys == ["agents/demo.zip"]
