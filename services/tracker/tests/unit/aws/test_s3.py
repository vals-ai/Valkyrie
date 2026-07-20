"""Unit tests for tracker S3 streaming uploads.

Run: uv run pytest tests/unit/aws/test_s3.py
"""

from collections.abc import AsyncIterator
from typing import Any

import pytest
from botocore.exceptions import ClientError

from tracker.aws import s3 as s3_module
from tracker.aws.s3 import upload_stream_to_s3
from tracker.exceptions import S3Error
from tracker.types import AWSCredentials


class FakeS3Client:
    def __init__(self, fail_on_part: int | None = None) -> None:
        self.fail_on_part = fail_on_part
        self.parts: list[tuple[int, bytes]] = []
        self.completed_parts: list[dict[str, Any]] | None = None
        self.aborted = False

    async def __aenter__(self) -> "FakeS3Client":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        pass

    async def create_multipart_upload(self, Bucket: str, Key: str) -> dict[str, str]:
        return {"UploadId": "upload-1"}

    async def upload_part(self, Bucket: str, Key: str, PartNumber: int, UploadId: str, Body: bytes) -> dict[str, str]:
        if self.fail_on_part == PartNumber:
            raise ClientError({"Error": {"Code": "500", "Message": "Error"}}, "UploadPart")
        self.parts.append((PartNumber, bytes(Body)))
        return {"ETag": f"etag-{PartNumber}"}

    async def complete_multipart_upload(
        self, Bucket: str, Key: str, UploadId: str, MultipartUpload: dict[str, Any]
    ) -> None:
        self.completed_parts = MultipartUpload["Parts"]

    async def abort_multipart_upload(self, Bucket: str, Key: str, UploadId: str) -> None:
        self.aborted = True


@pytest.fixture
def fake_client(monkeypatch: pytest.MonkeyPatch) -> FakeS3Client:
    client = FakeS3Client()

    def fake_s3_client(_aws: AWSCredentials) -> FakeS3Client:
        return client

    monkeypatch.setattr(s3_module, "s3_client", fake_s3_client)
    monkeypatch.setattr(s3_module, "_MULTIPART_PART_BYTES", 8)
    return client


class TestUploadStreamToS3:
    """Multipart streaming upload behavior."""

    async def test_splits_stream_into_parts_and_completes(
        self, fake_client: FakeS3Client, aws_credentials: AWSCredentials
    ) -> None:
        """
        Test cases:
        - Chunks accumulate until the part-size threshold, then flush as numbered parts.
        - The trailing partial buffer becomes the final part and the upload is completed.
        """

        async def chunks() -> AsyncIterator[bytes]:
            yield b"aaaa"
            yield b"bbbb"
            yield b"cc"

        total = await upload_stream_to_s3(chunks(), "key", aws_credentials, "bucket")

        assert total == 10
        assert fake_client.parts == [(1, b"aaaabbbb"), (2, b"cc")]
        assert fake_client.completed_parts == [
            {"ETag": "etag-1", "PartNumber": 1},
            {"ETag": "etag-2", "PartNumber": 2},
        ]
        assert not fake_client.aborted

    async def test_empty_stream_uploads_empty_object(
        self, fake_client: FakeS3Client, aws_credentials: AWSCredentials
    ) -> None:
        async def chunks() -> AsyncIterator[bytes]:
            return
            yield b""

        total = await upload_stream_to_s3(chunks(), "key", aws_credentials, "bucket")

        assert total == 0
        assert fake_client.parts == [(1, b"")]
        assert fake_client.completed_parts == [{"ETag": "etag-1", "PartNumber": 1}]

    async def test_aborts_multipart_upload_on_failure(
        self, monkeypatch: pytest.MonkeyPatch, aws_credentials: AWSCredentials
    ) -> None:
        """
        Test cases:
        - An upload failure aborts the multipart upload and surfaces as S3Error.
        """
        client = FakeS3Client(fail_on_part=1)

        def fake_s3_client(_aws: AWSCredentials) -> FakeS3Client:
            return client

        monkeypatch.setattr(s3_module, "s3_client", fake_s3_client)
        monkeypatch.setattr(s3_module, "_MULTIPART_PART_BYTES", 8)

        async def chunks() -> AsyncIterator[bytes]:
            yield b"aaaabbbb"

        with pytest.raises(S3Error, match="Failed to upload stream to S3"):
            await upload_stream_to_s3(chunks(), "key", aws_credentials, "bucket")

        assert client.aborted
        assert client.completed_parts is None
