# pyright: reportPrivateImportUsage=false, reportPrivateUsage=false

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

    def fake_s3_client(_aws: AWSCredentials, _bucket: str, _key: str) -> FakeS3Client:
        return client

    monkeypatch.setattr(s3_module, "_s3_client_for_object", fake_s3_client)
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

        def fake_s3_client(_aws: AWSCredentials, _bucket: str, _key: str) -> FakeS3Client:
            return client

        monkeypatch.setattr(s3_module, "_s3_client_for_object", fake_s3_client)
        monkeypatch.setattr(s3_module, "_MULTIPART_PART_BYTES", 8)

        async def chunks() -> AsyncIterator[bytes]:
            yield b"aaaabbbb"

        with pytest.raises(S3Error, match="Failed to upload stream to S3"):
            await upload_stream_to_s3(chunks(), "key", aws_credentials, "bucket")

        assert client.aborted
        assert client.completed_parts is None

    async def test_aborts_before_completion_when_authority_is_revoked(
        self,
        fake_client: FakeS3Client,
        aws_credentials: AWSCredentials,
    ) -> None:
        authority_checks = iter([True, False])

        async def chunks() -> AsyncIterator[bytes]:
            yield b"final"

        with pytest.raises(S3Error, match="authority was revoked"):
            await upload_stream_to_s3(
                chunks(),
                "key",
                aws_credentials,
                "bucket",
                should_continue=lambda: next(authority_checks),
            )

        assert fake_client.parts == [(1, b"final")]
        assert fake_client.completed_parts is None
        assert fake_client.aborted


class TestRuntimeCredentialSelection:
    """Hosted result objects use refreshable workload credentials within a tight boundary."""

    def test_uses_runtime_credentials_for_configured_benchmark_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VALKYRIE_USE_RUNTIME_S3_CREDENTIALS", "true")
        monkeypatch.setenv("AWS_S3_BUCKET", "agentic-harness-dev")

        assert s3_module._uses_runtime_credentials("agentic-harness-dev", "benchmarks/run-id/task-id/output.tar.gz")
        assert s3_module._uses_runtime_credentials("agentic-harness-dev", "benchmarks")

    @pytest.mark.parametrize(
        ("bucket", "key"),
        [
            ("another-bucket", "benchmarks/run-id/output.json"),
            ("agentic-harness-dev", "agents/terminus.zip"),
            ("agentic-harness-dev", "benchmark-lookalike/run-id/output.json"),
        ],
    )
    def test_keeps_caller_credentials_outside_runtime_boundary(
        self, monkeypatch: pytest.MonkeyPatch, bucket: str, key: str
    ) -> None:
        monkeypatch.setenv("VALKYRIE_USE_RUNTIME_S3_CREDENTIALS", "true")
        monkeypatch.setenv("AWS_S3_BUCKET", "agentic-harness-dev")

        assert not s3_module._uses_runtime_credentials(bucket, key)

    def test_runtime_mode_is_opt_in(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("VALKYRIE_USE_RUNTIME_S3_CREDENTIALS", raising=False)
        monkeypatch.setenv("AWS_S3_BUCKET", "agentic-harness-dev")

        assert not s3_module._uses_runtime_credentials("agentic-harness-dev", "benchmarks/run-id/output.json")

    def test_runtime_session_does_not_receive_static_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[dict[str, str]] = []

        class Session:
            def __init__(self, **kwargs: str) -> None:
                calls.append(kwargs)

        monkeypatch.setattr(s3_module.aioboto3, "Session", Session)
        s3_module._runtime_s3_session.cache_clear()

        s3_module._runtime_s3_session("us-east-1")

        assert calls == [{"region_name": "us-east-1"}]
        s3_module._runtime_s3_session.cache_clear()
