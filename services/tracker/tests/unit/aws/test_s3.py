"""Unit tests for tracker S3 streaming uploads.

Run: uv run pytest tests/unit/aws/test_s3.py
"""

from collections.abc import AsyncIterator
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from tracker.aws import s3 as s3_module
from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.aws.runtime import AWSRuntime
from tracker.aws.s3 import (
    S3ObjectStore,
    copy_s3_object,
    create_presigned_url,
    delete_from_s3,
    download_from_s3,
    download_many_from_s3,
    list_agents,
    list_s3_objects,
    s3_object_exists,
    upload_to_s3,
    upload_stream_to_s3,
)
from tracker.api.run_artifacts import get_run_artifact_url, list_run_artifacts
from tracker.exceptions import S3Error


class MockS3Client:
    """Record multipart upload operations without calling AWS."""

    def __init__(self, fail_on_part: int | None = None) -> None:
        self.fail_on_part = fail_on_part
        self.parts: list[tuple[int, bytes]] = []
        self.completed_parts: list[dict[str, Any]] | None = None
        self.aborted = False
        self.requests: dict[str, list[dict[str, Any]]] = {
            "create": [],
            "upload": [],
            "complete": [],
            "abort": [],
        }

    async def __aenter__(self) -> "MockS3Client":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        pass

    async def create_multipart_upload(self, **request: Any) -> dict[str, str]:
        self.requests["create"].append(request)
        return {"UploadId": "upload-1"}

    async def upload_part(self, **request: Any) -> dict[str, str]:
        self.requests["upload"].append(request)
        part_number = request["PartNumber"]
        if self.fail_on_part == part_number:
            raise ClientError({"Error": {"Code": "500", "Message": "Error"}}, "UploadPart")
        self.parts.append((part_number, bytes(request["Body"])))
        return {"ETag": f"etag-{part_number}"}

    async def complete_multipart_upload(self, **request: Any) -> None:
        self.requests["complete"].append(request)
        self.completed_parts = request["MultipartUpload"]["Parts"]

    async def abort_multipart_upload(self, **request: Any) -> None:
        self.requests["abort"].append(request)
        self.aborted = True


class DownloadBody:
    def __init__(self, content: bytes) -> None:
        self._content = content

    async def __aenter__(self) -> "DownloadBody":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        pass

    async def read(self) -> bytes:
        return self._content


class DownloadClient:
    def __init__(self, responses: dict[str, bytes | ClientError]) -> None:
        self._responses = responses
        self.keys: list[str] = []
        self.requests: list[dict[str, str]] = []
        self.entries = 0

    async def __aenter__(self) -> "DownloadClient":
        self.entries += 1
        return self

    async def __aexit__(self, *_exc: object) -> None:
        pass

    async def get_object(self, **request: str) -> dict[str, DownloadBody]:
        self.requests.append(request)
        key = request["Key"]
        self.keys.append(key)
        response = self._responses[key]
        if isinstance(response, ClientError):
            raise response
        return {"Body": DownloadBody(response)}


async def test_download_many_reuses_one_client_and_skips_provider_failures(
    monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime
) -> None:
    missing = ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")
    client = DownloadClient({"first": b"one", "missing": missing, "last": b"three"})
    monkeypatch.setattr(type(aws_runtime.clients), "s3_client", lambda _provider: client)

    downloaded = [item async for item in download_many_from_s3(["first", "missing", "last"], aws_runtime)]

    assert downloaded == [("first", b"one"), ("last", b"three")]
    assert client.keys == ["first", "missing", "last"]
    assert client.entries == 1


async def test_object_store_get_many_reuses_one_client_and_skips_provider_failures(
    monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime
) -> None:
    missing = ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")
    client = DownloadClient({"first": b"one", "missing": missing, "last": b"three"})
    monkeypatch.setattr(type(aws_runtime.clients), "s3_client", lambda _provider: client)

    async def keys() -> AsyncIterator[str]:
        yield "first"
        yield "missing"
        yield "last"

    downloaded = [item async for item in S3ObjectStore(aws_runtime).get_many(keys())]

    assert downloaded == [("first", b"one"), ("last", b"three")]
    assert client.keys == ["first", "missing", "last"]
    assert client.entries == 1


class ObjectListPaginator:
    def __init__(self, pages: list[dict[str, list[dict[str, object]]]]) -> None:
        self._pages = pages
        self.calls: list[dict[str, str]] = []

    async def paginate(self, **kwargs: str) -> AsyncIterator[dict[str, list[dict[str, object]]]]:
        self.calls.append(kwargs)
        for page in self._pages:
            yield page


class ReadClient(DownloadClient):
    def __init__(self, responses: dict[str, bytes | ClientError], paginator: ObjectListPaginator) -> None:
        super().__init__(responses)
        self._paginator = paginator

    def get_paginator(self, name: str) -> ObjectListPaginator:
        assert name == "list_objects_v2"
        return self._paginator


async def test_object_store_read_session_and_listing_preserve_object_metadata(
    monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime
) -> None:
    paginator = ObjectListPaginator(
        [{"Contents": [{"Key": "agents/alpha.zip"}, {"LastModified": "ignored"}, {"Key": "agents/beta.zip"}]}]
    )
    client = ReadClient({"agents/alpha.zip": b"alpha"}, paginator)
    monkeypatch.setattr(type(aws_runtime.clients), "s3_client", lambda _provider: client)

    async with S3ObjectStore(aws_runtime).read_session() as reader:
        assert await reader.get_bytes("agents/alpha.zip") == b"alpha"
        listed = [stored_object async for stored_object in reader.list_objects("agents/")]

    assert [stored_object.key for stored_object in listed] == ["agents/alpha.zip", "agents/beta.zip"]
    assert paginator.calls == [{"Bucket": "test-bucket", "Prefix": "agents/"}]
    assert client.entries == 1


async def test_managed_read_session_and_get_many_apply_owner_guard(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
) -> None:
    paginator = ObjectListPaginator([{"Contents": [{"Key": "agents/alpha.zip"}]}])
    client = ReadClient({"agents/alpha.zip": b"alpha"}, paginator)
    runtime = AWSRuntime(
        resources=aws_runtime.resources,
        clients=DefaultChainAWSClientProvider(region=aws_runtime.resources.region),
        expected_bucket_owner="123456789012",
    )

    def s3_client(_provider: DefaultChainAWSClientProvider) -> ReadClient:
        return client

    monkeypatch.setattr(DefaultChainAWSClientProvider, "s3_client", s3_client)

    async with S3ObjectStore(runtime).read_session() as reader:
        assert await reader.get_bytes("agents/alpha.zip") == b"alpha"
        assert [item.key async for item in reader.list_objects("agents/")] == ["agents/alpha.zip"]

    async def keys() -> AsyncIterator[str]:
        yield "agents/alpha.zip"

    assert [item async for item in S3ObjectStore(runtime).get_many(keys())] == [("agents/alpha.zip", b"alpha")]
    assert [item async for item in download_many_from_s3(["agents/alpha.zip"], runtime)] == [
        ("agents/alpha.zip", b"alpha")
    ]
    assert client.requests == [
        {"Bucket": "test-bucket", "Key": "agents/alpha.zip", "ExpectedBucketOwner": "123456789012"},
        {"Bucket": "test-bucket", "Key": "agents/alpha.zip", "ExpectedBucketOwner": "123456789012"},
        {"Bucket": "test-bucket", "Key": "agents/alpha.zip", "ExpectedBucketOwner": "123456789012"},
    ]
    assert paginator.calls == [{"Bucket": "test-bucket", "Prefix": "agents/", "ExpectedBucketOwner": "123456789012"}]


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
            expected_bucket_owner="123456789012",
        )

        result = await create_presigned_url("agents/demo.zip", runtime, expiration=86_400)

        assert result == "https://example.test/presigned"
        client.generate_presigned_url.assert_awaited_once_with(
            "get_object",
            Params={"Bucket": "test-bucket", "Key": "agents/demo.zip"},
            ExpiresIn=3_600,
        )


async def test_managed_direct_operations_apply_owner_guard(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
) -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.get_object.return_value = {"Body": DownloadBody(b"content")}
    client.copy_object.return_value = {"VersionId": "version-1"}
    paginator = ObjectListPaginator([{"Contents": [{"Key": "benchmarks/run/result.json"}, {"Key": "agents/demo.zip"}]}])
    client.get_paginator = MagicMock(return_value=paginator)
    runtime = AWSRuntime(
        resources=aws_runtime.resources,
        clients=DefaultChainAWSClientProvider(region=aws_runtime.resources.region),
        expected_bucket_owner="123456789012",
    )

    def s3_client(_provider: DefaultChainAWSClientProvider) -> AsyncMock:
        return client

    monkeypatch.setattr(DefaultChainAWSClientProvider, "s3_client", s3_client)

    await upload_to_s3(b"content", "key", runtime)
    assert await download_from_s3("key", runtime) == b"content"
    await delete_from_s3("key", runtime, version_id="version-1")
    assert await copy_s3_object("source", "destination", runtime) == "version-1"
    assert await s3_object_exists("key", runtime)
    assert [key async for key in list_s3_objects("benchmarks/", runtime)] == [
        "benchmarks/run/result.json",
        "agents/demo.zip",
    ]
    assert await list_agents(runtime) == [("demo", None)]

    owner = {"ExpectedBucketOwner": "123456789012"}
    client.put_object.assert_awaited_once_with(Bucket="test-bucket", Key="key", Body=b"content", **owner)
    client.get_object.assert_awaited_once_with(Bucket="test-bucket", Key="key", **owner)
    client.delete_object.assert_awaited_once_with(Bucket="test-bucket", Key="key", VersionId="version-1", **owner)
    client.copy_object.assert_awaited_once_with(
        Bucket="test-bucket",
        CopySource={"Bucket": "test-bucket", "Key": "source"},
        Key="destination",
        **owner,
    )
    client.head_object.assert_awaited_once_with(Bucket="test-bucket", Key="key", **owner)
    assert paginator.calls == [
        {"Bucket": "test-bucket", "Prefix": "benchmarks/", **owner},
        {"Bucket": "test-bucket", "Prefix": "agents/", **owner},
    ]


async def test_explicit_credential_upload_does_not_add_owner_guard(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
) -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client

    def s3_client(_provider: object) -> AsyncMock:
        return client

    monkeypatch.setattr(type(aws_runtime.clients), "s3_client", s3_client)

    await upload_to_s3(b"content", "key", aws_runtime)

    client.put_object.assert_awaited_once_with(Bucket="test-bucket", Key="key", Body=b"content")


async def test_managed_run_artifact_calls_guard_aws_without_changing_presigned_url_contract(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
) -> None:
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.list_objects_v2.return_value = {"Contents": []}
    client.head_object.return_value = {"ContentLength": 7}
    client.generate_presigned_url.return_value = "https://download.test/file"
    runtime = AWSRuntime(
        resources=aws_runtime.resources,
        clients=DefaultChainAWSClientProvider(region=aws_runtime.resources.region),
        expected_bucket_owner="123456789012",
    )
    run_context = MagicMock(aws_runtime=runtime)

    def s3_client(_provider: DefaultChainAWSClientProvider) -> AsyncMock:
        return client

    monkeypatch.setattr(DefaultChainAWSClientProvider, "s3_client", s3_client)
    benchmark_id = uuid4()
    root = f"benchmarks/{benchmark_id}/"

    await list_run_artifacts(benchmark_id, cast(Any, run_context), prefix="", cursor=None, limit=100)
    response = await get_run_artifact_url(benchmark_id, cast(Any, run_context), path="result.json")

    owner = {"ExpectedBucketOwner": "123456789012"}
    client.list_objects_v2.assert_awaited_once_with(
        Bucket="test-bucket",
        Prefix=root,
        MaxKeys=100,
        **owner,
    )
    client.head_object.assert_awaited_once_with(
        Bucket="test-bucket",
        Key=root + "result.json",
        **owner,
    )
    client.generate_presigned_url.assert_awaited_once_with(
        "get_object",
        Params={"Bucket": "test-bucket", "Key": root + "result.json"},
        ExpiresIn=300,
    )
    assert response.download_url == "https://download.test/file"


async def test_versioned_copy_can_be_deleted_exactly(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
) -> None:
    client = AsyncMock()
    client.copy_object.return_value = {"VersionId": "version-1"}
    client_context = AsyncMock()
    client_context.__aenter__.return_value = client

    def s3_client(_provider: object) -> AsyncMock:
        return client_context

    monkeypatch.setattr(type(aws_runtime.clients), "s3_client", s3_client)

    version_id = await copy_s3_object("agents/demo.zip", "benchmarks/run/demo.zip", aws_runtime)
    await delete_from_s3("benchmarks/run/demo.zip", aws_runtime, version_id=version_id)

    assert version_id == "version-1"
    client.delete_object.assert_awaited_once_with(
        Bucket="test-bucket",
        Key="benchmarks/run/demo.zip",
        VersionId="version-1",
    )


async def test_delete_from_s3_translates_aws_errors(
    monkeypatch: pytest.MonkeyPatch,
    aws_runtime: AWSRuntime,
) -> None:
    client = AsyncMock()
    client.delete_object.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "denied"}},
        "DeleteObject",
    )
    client_context = AsyncMock()
    client_context.__aenter__.return_value = client

    def s3_client(_provider: object) -> AsyncMock:
        return client_context

    monkeypatch.setattr(type(aws_runtime.clients), "s3_client", s3_client)

    with pytest.raises(S3Error, match="Failed to delete object from S3"):
        await delete_from_s3("benchmarks/run/demo.zip", aws_runtime)


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

    async def test_object_store_preserves_multipart_behavior(
        self, mock_s3_client: MockS3Client, aws_runtime: AWSRuntime
    ) -> None:
        async def chunks() -> AsyncIterator[bytes]:
            yield b"aaaa"
            yield b"bbbb"
            yield b"cc"

        total = await S3ObjectStore(aws_runtime).put_stream("key", chunks())

        assert total == 10
        assert mock_s3_client.parts == [(1, b"aaaabbbb"), (2, b"cc")]
        assert mock_s3_client.completed_parts == [
            {"ETag": "etag-1", "PartNumber": 1},
            {"ETag": "etag-2", "PartNumber": 2},
        ]

    async def test_managed_upload_guards_every_multipart_operation(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        client = MockS3Client()
        runtime = AWSRuntime(
            resources=aws_runtime.resources,
            clients=DefaultChainAWSClientProvider(region=aws_runtime.resources.region),
            expected_bucket_owner="123456789012",
        )

        def s3_client(_provider: DefaultChainAWSClientProvider) -> MockS3Client:
            return client

        monkeypatch.setattr(DefaultChainAWSClientProvider, "s3_client", s3_client)

        async def chunks() -> AsyncIterator[bytes]:
            yield b"content"

        await upload_stream_to_s3(chunks(), "key", runtime)

        owner = {"ExpectedBucketOwner": "123456789012"}
        assert client.requests["create"] == [{"Bucket": "test-bucket", "Key": "key", **owner}]
        assert client.requests["upload"] == [
            {
                "Bucket": "test-bucket",
                "Key": "key",
                "PartNumber": 1,
                "UploadId": "upload-1",
                "Body": b"content",
                **owner,
            }
        ]
        assert client.requests["complete"] == [
            {
                "Bucket": "test-bucket",
                "Key": "key",
                "UploadId": "upload-1",
                "MultipartUpload": {"Parts": [{"ETag": "etag-1", "PartNumber": 1}]},
                **owner,
            }
        ]

    async def test_object_store_translates_and_aborts_multipart_upload_on_failure(
        self, monkeypatch: pytest.MonkeyPatch, aws_runtime: AWSRuntime
    ) -> None:
        """
        Test cases:
        - An object-store upload failure aborts the multipart upload and surfaces as S3Error.
        """
        client = MockS3Client(fail_on_part=1)

        def s3_client(_provider: object) -> MockS3Client:
            return client

        monkeypatch.setattr(type(aws_runtime.clients), "s3_client", s3_client)
        monkeypatch.setattr(s3_module, "_MULTIPART_PART_BYTES", 8)

        async def chunks() -> AsyncIterator[bytes]:
            yield b"aaaabbbb"

        with pytest.raises(S3Error, match="Failed to upload stream to S3"):
            await S3ObjectStore(aws_runtime).put_stream("key", chunks())

        assert client.aborted
        assert client.completed_parts is None

    async def test_managed_multipart_abort_applies_owner_guard(
        self,
        monkeypatch: pytest.MonkeyPatch,
        aws_runtime: AWSRuntime,
    ) -> None:
        client = MockS3Client(fail_on_part=1)
        runtime = AWSRuntime(
            resources=aws_runtime.resources,
            clients=DefaultChainAWSClientProvider(region=aws_runtime.resources.region),
            expected_bucket_owner="123456789012",
        )

        def s3_client(_provider: DefaultChainAWSClientProvider) -> MockS3Client:
            return client

        monkeypatch.setattr(DefaultChainAWSClientProvider, "s3_client", s3_client)

        async def chunks() -> AsyncIterator[bytes]:
            yield b"content"

        with pytest.raises(S3Error):
            await upload_stream_to_s3(chunks(), "key", runtime)

        assert client.requests["abort"] == [
            {
                "Bucket": "test-bucket",
                "Key": "key",
                "UploadId": "upload-1",
                "ExpectedBucketOwner": "123456789012",
            }
        ]

    async def test_aborts_before_completion_when_authority_is_revoked(
        self,
        mock_s3_client: MockS3Client,
        aws_runtime: AWSRuntime,
    ) -> None:
        """
        Test cases:
        - Revoked upload authority aborts the multipart upload before completion.
        """
        authority_checks = iter([True, False])

        async def chunks() -> AsyncIterator[bytes]:
            yield b"final"

        with pytest.raises(S3Error, match="authority was revoked"):
            await upload_stream_to_s3(
                chunks(),
                "key",
                aws_runtime,
                should_continue=lambda: next(authority_checks),
            )

        assert mock_s3_client.parts == [(1, b"final")]
        assert mock_s3_client.completed_parts is None
        assert mock_s3_client.aborted
