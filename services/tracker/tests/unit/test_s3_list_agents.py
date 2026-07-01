from datetime import datetime, timezone
from types import TracebackType
from typing import Any

import pytest

import tracker.aws.s3 as s3_module
from tracker.types import AWSCredentials


class FakeS3Client:
    def __init__(self, pages: list[list[str]]) -> None:
        self._pages = pages

    async def __aenter__(self) -> "FakeS3Client":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    def get_paginator(self, _name: str) -> "FakeS3Client":
        return self

    async def paginate(self, **_kwargs: Any) -> Any:
        for keys in self._pages:
            yield {
                "Contents": [{"Key": key, "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc)} for key in keys]
            }


@pytest.mark.asyncio
async def test_list_agents_filters_nested_version_keys_and_reads_all_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeS3Client(
        [
            ["agents/first.zip", "agents/first/1.0.0.zip"],
            ["agents/second.zip", "agents/ignored/readme.txt"],
        ]
    )
    monkeypatch.setattr(s3_module, "_s3_client", lambda _aws: client)

    agents = await s3_module.list_agents(
        aws=AWSCredentials(
            aws_access_key_id="test",
            aws_secret_access_key="test",
            aws_default_region="us-east-1",
        ),
        s3_bucket="test-bucket",
    )

    assert [name for name, _ in agents] == ["first", "second"]
