"""Versioned provider fixture shared by PostgreSQL and boundary tests."""

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock

from botocore.exceptions import ClientError


class VersionStore:
    def __init__(self, run_id: str) -> None:
        self.fence_statement: dict[str, Any] | None = None
        self.tags: dict[str, list[dict[str, str]]] = {}
        self.versioning: dict[str, dict[str, str]] = {}
        self.region = "us-east-1"
        self.unrelated: dict[tuple[str, str], bytes] = {}
        self.execution_objects: dict[tuple[str, str], tuple[str | None, bytes]] = {}
        self.key = f"benchmarks/{run_id}/results.json"
        self.versions: dict[str, list[tuple[str, bytes | None]]] = {
            "source": [("s1", b'{"value":1}')],
            "destination": [("d1", b'{"value":1}')],
        }

    async def __aenter__(self) -> "VersionStore":
        return self

    async def __aexit__(self, *_arguments: object) -> None:
        pass

    async def head_bucket(self, **arguments: Any) -> dict[str, str]:
        assert arguments["ExpectedBucketOwner"] == "123456789012"
        return {"BucketRegion": self.region}

    async def get_bucket_tagging(self, **arguments: Any) -> dict[str, Any]:
        assert arguments["ExpectedBucketOwner"] == "123456789012"
        if arguments["Bucket"] not in self.tags:
            raise ClientError({"Error": {"Code": "NoSuchTagSet"}}, "GetBucketTagging")
        return {"TagSet": self.tags[arguments["Bucket"]]}

    async def get_bucket_versioning(self, **arguments: Any) -> dict[str, str]:
        assert arguments["ExpectedBucketOwner"] == "123456789012"
        return self.versioning.get(arguments["Bucket"], {"Status": "Enabled"})

    async def get_bucket_policy(self, **arguments: Any) -> dict[str, str]:
        assert arguments["ExpectedBucketOwner"] == "123456789012"
        return {"Policy": json.dumps({"Statement": [] if self.fence_statement is None else [self.fence_statement]})}

    async def list_object_versions(self, **arguments: Any) -> dict[str, Any]:
        assert arguments["ExpectedBucketOwner"] == "123456789012"
        versions: list[dict[str, Any]] = []
        markers: list[dict[str, Any]] = []
        for index, (identifier, content) in enumerate(self.versions[arguments["Bucket"]]):
            item = {
                "Key": self.key,
                "VersionId": identifier,
                "Size": len(content or b""),
                "IsLatest": index == 0,
                "LastModified": datetime(2026, 1, 1, tzinfo=UTC) - timedelta(seconds=index),
            }
            (markers if content is None else versions).append(item)
        for (bucket, key), content in self.unrelated.items():
            if bucket == arguments["Bucket"] and key.startswith(arguments["Prefix"]):
                versions.append(
                    {
                        "Key": key,
                        "VersionId": "unrelated",
                        "Size": len(content),
                        "IsLatest": True,
                        "LastModified": datetime(2026, 1, 1, tzinfo=UTC),
                    }
                )
        return {"Versions": versions, "DeleteMarkers": markers, "IsTruncated": False}

    async def list_multipart_uploads(self, **_arguments: Any) -> dict[str, Any]:
        return {"Uploads": [], "IsTruncated": False}

    async def get_object(self, **arguments: Any) -> dict[str, Any]:
        execution_object = self.execution_objects.get((arguments["Bucket"], arguments["Key"]))
        if execution_object is not None:
            identifier, content = execution_object
            stream = AsyncMock()
            stream.__aenter__.return_value = stream
            stream.read.return_value = content
            return {"Body": stream, "ContentLength": len(content), **({"VersionId": identifier} if identifier else {})}
        content = next(
            content
            for identifier, content in self.versions[arguments["Bucket"]]
            if identifier == arguments["VersionId"]
        )
        assert content is not None
        stream = AsyncMock()
        stream.__aenter__.return_value = stream
        stream.read.return_value = content
        response: dict[str, Any] = {"Body": stream, "ContentLength": len(content)}
        if arguments["VersionId"] != "null" or self.versioning.get(arguments["Bucket"]) != {}:
            response["VersionId"] = arguments["VersionId"]
        return response
