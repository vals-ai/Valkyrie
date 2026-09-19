"""Local agent-library storage at the AWS client boundary."""

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from botocore.exceptions import ClientError
from fastapi import FastAPI
from tracker.aws.clients import ExplicitCredentialsAWSClientProvider


@pytest.fixture
def agent_library(local_tracker_app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> dict[str, bytes]:
    """Exercise CLI, SDK, Tracker, and S3 helpers with local object storage."""
    objects: dict[str, bytes] = {}
    parts: list[bytes] = []
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.create_multipart_upload.return_value = {"UploadId": "local-upload"}

    async def upload_part(**kwargs: Any) -> dict[str, str]:
        parts.append(kwargs["Body"])

        return {"ETag": "local-etag"}

    async def complete(**kwargs: Any) -> dict[str, str]:
        objects[kwargs["Key"]] = b"".join(parts)
        parts.clear()

        return {}

    async def head(**kwargs: Any) -> dict[str, int]:
        if kwargs["Key"] not in objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

        return {"ContentLength": len(objects[kwargs["Key"]])}

    async def delete(**kwargs: Any) -> dict[str, str]:
        del objects[kwargs["Key"]]

        return {}

    async def pages(**_kwargs: Any) -> AsyncIterator[dict[str, object]]:
        yield {"Contents": [{"Key": key} for key in objects]}

    client.upload_part.side_effect = upload_part
    client.complete_multipart_upload.side_effect = complete
    client.head_object.side_effect = head
    client.delete_object.side_effect = delete
    client.generate_presigned_url.return_value = "https://download.test/agents/alias.zip"
    paginator = MagicMock()
    paginator.paginate.side_effect = pages
    client.get_paginator = MagicMock(return_value=paginator)

    def s3_client(_provider: ExplicitCredentialsAWSClientProvider) -> AsyncMock:
        return client

    async def handle(_transport: httpx.AsyncHTTPTransport, request: httpx.Request) -> httpx.Response:
        if request.url.host == "download.test":
            assert "authorization" not in request.headers
            assert not any(header.startswith("x-harness") for header in request.headers)

            return httpx.Response(200, content=objects[request.url.path.lstrip("/")])
        async with httpx.ASGITransport(app=local_tracker_app) as transport:
            return await transport.handle_async_request(request)

    monkeypatch.setattr(ExplicitCredentialsAWSClientProvider, "s3_client", s3_client)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", handle)

    return objects
