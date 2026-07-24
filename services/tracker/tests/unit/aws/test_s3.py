"""Unit tests for tracker S3 streaming uploads.

Run: uv run pytest tests/unit/aws/test_s3.py
"""

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import ClientError

from tracker.aws import s3 as s3_module
from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import (
    create_presigned_url,
    download_range_from_s3,
    upload_stream_to_s3,
    upload_to_s3_if_absent,
)
from tracker.exceptions import S3Error, S3ObjectExistsError


class MockS3Client:
    """Record multipart upload operations without calling AWS."""

    def __init__(self, fail_on_part: int | None = None) -> None:
        self.fail_on_part = fail_on_part
        self.parts: list[tuple[int, bytes]] = []
        self.completed_parts: list[dict[str, Any]] | None = None
        self.aborted = False

    async def __aenter__(self) -> "MockS3Client":
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


class TestCreatePresignedUrl:
    """Presigned URL lifetime behavior."""

    async def test_default_chain_ttl_is_applied_to_s3(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        client = AsyncMock()
        client.generate_presigned_url.return_value = "https://example.test/presigned"
        client_context = AsyncMock()
        client_context.__aenter__.return_value = client

        def s3_client(_provider: DefaultChainAWSClientProvider) -> AsyncMock:
            return client_context

        monkeypatch.setattr(DefaultChainAWSClientProvider, "s3_client", s3_client)
        runtime = AWSRuntime(
            resources=aws_runtime.resources,
            clients=DefaultChainAWSClientProvider(region=aws_runtime.resources.region),
        )

        result = await create_presigned_url("agents/demo.zip", runtime, expiration=86_400)

        assert result == "https://example.test/presigned"
        client.generate_presigned_url.assert_awaited_once_with(
            "get_object",
            Params={"Bucket": "test-bucket", "Key": "agents/demo.zip"},
            ExpiresIn=3_600,
        )


async def test_download_range_uses_one_inclusive_get(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
) -> None:
    body = AsyncMock()
    body.__aenter__.return_value = body
    body.read.return_value = b"content"
    client = AsyncMock()
    client.get_object.return_value = {"Body": body}
    context = AsyncMock()
    context.__aenter__.return_value = client

    def s3_client(_provider: object) -> AsyncMock:
        return context

    monkeypatch.setattr(type(aws_runtime.clients), "s3_client", s3_client)

    content = await download_range_from_s3("pack", 10, 19, aws_runtime)

    assert content == b"content"
    client.get_object.assert_awaited_once_with(
        Bucket="test-bucket",
        Key="pack",
        Range="bytes=10-19",
    )


async def test_conditional_upload_never_overwrites_existing_object(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
) -> None:
    client = AsyncMock()
    client.put_object.side_effect = ClientError(
        {"Error": {"Code": "PreconditionFailed", "Message": "exists"}},
        "PutObject",
    )
    context = AsyncMock()
    context.__aenter__.return_value = client

    def s3_client(_provider: object) -> AsyncMock:
        return context

    monkeypatch.setattr(type(aws_runtime.clients), "s3_client", s3_client)

    with pytest.raises(S3ObjectExistsError):
        await upload_to_s3_if_absent(b"index", "index.json", aws_runtime)

    client.put_object.assert_awaited_once_with(
        Bucket="test-bucket",
        Key="index.json",
        Body=b"index",
        IfNoneMatch="*",
    )


@pytest.fixture
def mock_s3_client(monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime) -> MockS3Client:
    client = MockS3Client()

    def s3_client(_provider: object) -> MockS3Client:
        return client

    monkeypatch.setattr(type(aws_runtime.clients), "s3_client", s3_client)
    monkeypatch.setattr(s3_module, "_MULTIPART_PART_BYTES", 8)
    return client


class TestUploadStreamToS3:
    """Multipart streaming upload behavior."""

    async def test_splits_stream_into_parts_and_completes(
        self, mock_s3_client: MockS3Client, aws_runtime: AWSRuntime
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

        total = await upload_stream_to_s3(chunks(), "key", aws_runtime)

        assert total == 10
        assert mock_s3_client.parts == [(1, b"aaaabbbb"), (2, b"cc")]
        assert mock_s3_client.completed_parts == [
            {"ETag": "etag-1", "PartNumber": 1},
            {"ETag": "etag-2", "PartNumber": 2},
        ]
        assert not mock_s3_client.aborted

    async def test_aborts_multipart_upload_on_failure(
        self, monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime
    ) -> None:
        """
        Test cases:
        - An upload failure aborts the multipart upload and surfaces as S3Error.
        """
        client = MockS3Client(fail_on_part=1)

        def s3_client(_provider: object) -> MockS3Client:
            return client

        monkeypatch.setattr(type(aws_runtime.clients), "s3_client", s3_client)
        monkeypatch.setattr(s3_module, "_MULTIPART_PART_BYTES", 8)

        async def chunks() -> AsyncIterator[bytes]:
            yield b"aaaabbbb"

        with pytest.raises(S3Error, match="Failed to upload stream to S3"):
            await upload_stream_to_s3(chunks(), "key", aws_runtime)

        assert client.aborted
        assert client.completed_parts is None
