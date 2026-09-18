"""In-memory S3 boundary for run lifecycle tests."""

from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock

from botocore.exceptions import ClientError

from tests.utils import TEST_ORG_ID


class MemoryS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def __aenter__(self) -> "MemoryS3":
        return self

    async def __aexit__(self, *_arguments: object) -> None:
        pass

    def record(self, operation: str, arguments: dict[str, Any]) -> None:
        self.calls.append((operation, arguments))
        assert arguments["ExpectedBucketOwner"] == "123456789012"

    async def head_bucket(self, **arguments: Any) -> dict[str, str]:
        self.record("head_bucket", arguments)
        return {"BucketRegion": "us-east-1"}

    async def get_bucket_tagging(self, **arguments: Any) -> dict[str, Any]:
        self.record("tags", arguments)
        return {
            "TagSet": [
                {"Key": "valsmith:environment", "Value": "dev"},
                {"Key": "valsmith:owner-account-id", "Value": "123"},
                {"Key": "valsmith:backup", "Value": "true"},
                {"Key": "valsmith:valkyrie-org-id", "Value": str(TEST_ORG_ID)},
            ]
        }

    async def get_bucket_versioning(self, **arguments: Any) -> dict[str, str]:
        self.record("versioning", arguments)
        return {"Status": "Enabled"}

    async def head_object(self, **arguments: Any) -> dict[str, Any]:
        self.record("head", arguments)
        location = (arguments["Bucket"], arguments["Key"])
        if location not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

        return {"ContentLength": len(self.objects[location]), "ETag": '"frozen-etag"'}

    async def get_object(self, **arguments: Any) -> dict[str, Any]:
        self.record("get", arguments)
        stream = AsyncMock()
        stream.__aenter__.return_value = stream
        stream.read.return_value = self.objects[arguments["Bucket"], arguments["Key"]]
        return {"Body": stream}

    async def put_object(self, **arguments: Any) -> dict[str, str]:
        self.record("put", arguments)
        self.objects[arguments["Bucket"], arguments["Key"]] = arguments["Body"]
        return {"VersionId": "saved-version"}

    async def copy_object(self, **arguments: Any) -> dict[str, str]:
        self.record("copy", arguments)
        source = arguments["CopySource"]
        self.objects[arguments["Bucket"], arguments["Key"]] = self.objects[source["Bucket"], source["Key"]]
        return {"VersionId": "copied-version"}

    async def list_objects_v2(self, **arguments: Any) -> dict[str, Any]:
        self.record("list", arguments)
        return {
            "Contents": [
                {"Key": key, "Size": len(content)}
                for (bucket, key), content in self.objects.items()
                if bucket == arguments["Bucket"] and key.startswith(arguments["Prefix"])
            ]
        }

    def get_paginator(self, operation: str) -> "MemoryS3":
        assert operation == "list_objects_v2"
        return self

    async def paginate(self, **arguments: Any) -> AsyncIterator[dict[str, Any]]:
        yield await self.list_objects_v2(**arguments)

    async def generate_presigned_url(self, operation: str, *, Params: dict[str, str], ExpiresIn: int) -> str:
        assert operation == "get_object"
        assert set(Params) == {"Bucket", "Key"}
        self.calls.append(("presign", Params))
        return f"https://{Params['Bucket']}.example/{Params['Key']}?expires={ExpiresIn}"
