"""Exact version history and JSON transform verification at the provider boundary."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from tracker.lifecycle import LifecycleConflict
from tracker.run_relocation.providers import RelocationAWSBoundary, rewrite_json
from tracker.storage_migration_exchange import JsonLocatorEdit, ObjectTransformation, TrackerRequest, canonical_digest


def checksum(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class VersionStore:
    def __init__(self, run_id: str) -> None:
        self.fence_statement: dict[str, Any] | None = None
        self.key = f"benchmarks/{run_id}/results.json"
        self.versions: dict[str, list[tuple[str, bytes | None]]] = {
            "source": [("s1", b'{"value":1}')],
            "destination": [("d1", b'{"value":1}')],
        }

    async def __aenter__(self) -> "VersionStore":
        return self

    async def __aexit__(self, *_arguments: object) -> None:
        pass

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
        return {"Versions": versions, "DeleteMarkers": markers, "IsTruncated": False}

    async def list_multipart_uploads(self, **_arguments: Any) -> dict[str, Any]:
        return {"Uploads": [], "IsTruncated": False}

    async def get_object(self, **arguments: Any) -> dict[str, Any]:
        content = next(
            content
            for identifier, content in self.versions[arguments["Bucket"]]
            if identifier == arguments["VersionId"]
        )
        assert content is not None
        stream = AsyncMock()
        stream.__aenter__.return_value = stream
        stream.read.return_value = content
        return {"VersionId": arguments["VersionId"], "Body": stream, "ContentLength": len(content)}


def setup() -> tuple[RelocationAWSBoundary, VersionStore, dict[str, Any]]:
    run_id = str(uuid4())
    store = VersionStore(run_id)
    resources = {"region": "us-east-1", "s3_bucket": "source", "log_group": "runs", "log_retention_days": 7}
    identity: dict[str, Any] = {
        "operation_id": str(uuid4()),
        "parent_plan_sha256": "a" * 64,
        "github_owner_id": 42,
        "org_id": str(uuid4()),
        "source_aws_account_id": "123456789012",
        "destination_aws_account_id": "123456789012",
        "region": "us-east-1",
        "environment": "dev",
        "database_target": "postgresql:localhost:5432/test",
        "run_ids": [run_id],
    }
    request = {
        **{key: value for key, value in identity.items() if key not in {"operation_id", "parent_plan_sha256"}},
        "nonce": str(uuid4()),
        "action": "relocate",
        "plan": {
            "identity": identity,
            "runs": [
                {
                    "scope": {
                        "run_id": run_id,
                        "original_resources": resources,
                        "object_prefix": f"benchmarks/{run_id}/",
                        "log_group": f"runs/{run_id}",
                    },
                    "destination_resources": {**resources, "s3_bucket": "destination"},
                    "expected_label": None,
                    "execution_policy": "history_only",
                    "execution_arguments_sha256": "c" * 64,
                }
            ],
        },
        "copied_objects": [
            {
                "run_id": run_id,
                "key": store.key,
                "source_bucket": "source",
                "source_version_id": "s1",
                "destination_bucket": "destination",
                "destination_version_id": "d1",
                "is_delete_marker": False,
                "source_sha256": checksum(b'{"value":1}'),
                "destination_sha256": checksum(b'{"value":1}'),
                "source_size": 11,
                "destination_size": 11,
                "is_current": True,
            }
        ],
        "destination_versions": [
            {
                "run_id": run_id,
                "bucket": "destination",
                "key": store.key,
                "version_id": "d1",
                "is_delete_marker": False,
                "size": 11,
                "sha256": checksum(b'{"value":1}'),
                "is_current": True,
                "provenance": "copied",
            }
        ],
    }
    store.fence_statement = {
        "Sid": "ValSmithOwnerMigration" + identity["operation_id"].replace("-", ""),
        "Effect": "Deny",
        "Principal": "*",
        "Action": ["s3:PutObject", "s3:DeleteObject"],
        "Resource": [f"arn:aws:s3:::source/benchmarks/{run_id}/*"],
    }
    clients = Mock()
    clients.with_region.return_value = clients
    clients.s3_client.return_value = store
    return RelocationAWSBoundary(clients), store, request


@pytest.mark.asyncio
async def test_complete_history_is_verified_and_missing_or_changed_versions_fail() -> None:
    boundary, store, request = setup()
    parsed = TrackerRequest.model_validate(request)
    assert parsed.plan is not None
    await boundary.verify_objects(parsed, parsed.plan.runs[0])
    store.versions["destination"].append(("unplanned", b"hidden history"))
    with pytest.raises(LifecycleConflict):
        await boundary.verify_objects(parsed, parsed.plan.runs[0])
    store.versions["destination"] = [("d1", b"wrong bytes")]
    with pytest.raises(LifecycleConflict):
        await boundary.verify_objects(parsed, parsed.plan.runs[0])


@pytest.mark.asyncio
async def test_missing_source_copy_proof_and_premature_release_fail() -> None:
    boundary, _, request = setup()
    parsed = TrackerRequest.model_validate(request)
    assert parsed.plan is not None
    with pytest.raises(LifecycleConflict):
        await boundary.verify_objects(parsed, parsed.plan.runs[0], source_removed=True)
    request["copied_objects"] = []
    with pytest.raises(LifecycleConflict):
        await boundary.verify_objects(TrackerRequest.model_validate(request), parsed.plan.runs[0])


def test_planned_json_edits_are_exact_and_reject_ambiguous_objects() -> None:

    original = b'{"z": 1, "url": "s3://source/key"}'
    rewritten = b'{"z":1,"url":"s3://destination/key"}'
    transform = ObjectTransformation(
        source_bucket="source",
        key="key",
        source_version_id="v1",
        original_size=len(original),
        original_sha256=checksum(original),
        rewritten_size=len(rewritten),
        rewritten_sha256=checksum(rewritten),
        edits=(JsonLocatorEdit(pointer="/url", original="s3://source/key", replacement="s3://destination/key"),),
    )
    assert rewrite_json(original, transform) == rewritten
    for invalid in (b'{"url":"s3://source/key","url":"other"}', b'{"url":NaN}', b'{"url":"changed"}'):
        with pytest.raises(LifecycleConflict):
            rewrite_json(invalid, transform)


@pytest.mark.asyncio
async def test_collision_restoration_preserves_existing_current_and_all_history() -> None:
    boundary, store, request = setup()
    store.versions["destination"] = [("restored", b"existing"), ("d1", b'{"value":1}'), ("old", b"existing")]
    copied = request["destination_versions"][0]
    copied["is_current"] = False
    request["copied_objects"][0]["is_current"] = False
    request["destination_versions"] = [
        {
            **copied,
            "version_id": "restored",
            "size": 8,
            "sha256": checksum(b"existing"),
            "is_current": True,
            "provenance": "restored",
            "restored_from_version_id": "old",
        },
        copied,
        {**copied, "version_id": "old", "size": 8, "sha256": checksum(b"existing"), "provenance": "existing"},
    ]
    parsed = TrackerRequest.model_validate(request)
    assert parsed.plan is not None
    await boundary.verify_objects(parsed, parsed.plan.runs[0])
    request["destination_versions"][0]["restored_from_version_id"] = "d1"
    with pytest.raises(LifecycleConflict):
        await boundary.verify_objects(TrackerRequest.model_validate(request), parsed.plan.runs[0])


@pytest.mark.asyncio
async def test_current_delete_marker_is_proved_and_extra_source_marker_is_refused() -> None:
    boundary, store, request = setup()
    store.versions = {"source": [("s1", None)], "destination": [("d1", None)]}
    request["copied_objects"][0].update(
        is_delete_marker=True, source_size=0, destination_size=0, source_sha256=None, destination_sha256=None
    )
    request["destination_versions"][0].update(is_delete_marker=True, size=0, sha256=None)
    parsed = TrackerRequest.model_validate(request)
    assert parsed.plan is not None
    await boundary.verify_objects(parsed, parsed.plan.runs[0])
    store.versions["source"].append(("lost-marker", None))
    with pytest.raises(LifecycleConflict):
        await boundary.verify_objects(parsed, parsed.plan.runs[0])


@pytest.mark.asyncio
async def test_producer_transformed_fixture_is_verified_by_consumer() -> None:

    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "storage-migration-relocate-v1.json"
    request = TrackerRequest.model_validate_json(fixture.read_bytes())
    assert request.plan is not None
    transformation = request.plan.runs[0].transformations[0]
    original = json.dumps(
        {
            "artifact_uri": "s3://legacy-storage/benchmarks/11111111-1111-4111-8111-111111111111/artifact.txt",
            "unchanged": "example",
        }
    ).encode()
    rewritten = rewrite_json(original, transformation)
    copied = request.copied_objects[0]
    assert checksum(rewritten) == copied.destination_sha256
    assert canonical_digest(transformation.model_dump(mode="json")) == copied.transformation_sha256
    store = VersionStore(str(request.run_ids[0]))
    store.key = copied.key
    store.versions = {
        copied.source_bucket: [(copied.source_version_id, original)],
        copied.destination_bucket: [(copied.destination_version_id, rewritten)],
    }
    store.fence_statement = {
        "Sid": "ValSmithOwnerMigration" + request.plan.identity.operation_id.hex,
        "Effect": "Deny",
        "Principal": "*",
        "Action": ["s3:PutObject", "s3:DeleteObject"],
        "Resource": [f"arn:aws:s3:::{copied.source_bucket}/{request.plan.runs[0].scope.object_prefix}*"],
    }
    clients = Mock()
    clients.with_region.return_value = clients
    clients.s3_client.return_value = store
    await RelocationAWSBoundary(clients).verify_objects(request, request.plan.runs[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["absent", "put_only", "condition", "other_scope"])
async def test_copy_verification_requires_current_exact_source_fence(change: str) -> None:
    boundary, store, request = setup()
    assert store.fence_statement is not None
    if change == "absent":
        store.fence_statement = None
    elif change == "put_only":
        store.fence_statement["Action"] = "s3:PutObject"
    elif change == "condition":
        store.fence_statement["Condition"] = {"Bool": {"aws:SecureTransport": "false"}}
    else:
        store.fence_statement["Resource"] = ["arn:aws:s3:::source/unrelated/*"]
    parsed = TrackerRequest.model_validate(request)
    assert parsed.plan is not None
    with pytest.raises(LifecycleConflict):
        await boundary.verify_objects(parsed, parsed.plan.runs[0])


@pytest.mark.asyncio
async def test_copy_history_cannot_invert_source_version_order() -> None:
    boundary, store, request = setup()
    store.versions["source"] = [("s2", b"older"), ("s1", b'{"value":1}')]
    store.versions["destination"].append(("d2", b"older"))
    request["copied_objects"].append(
        {
            **request["copied_objects"][0],
            "source_version_id": "s2",
            "destination_version_id": "d2",
            "source_size": 5,
            "destination_size": 5,
            "source_sha256": checksum(b"older"),
            "destination_sha256": checksum(b"older"),
            "is_current": False,
        }
    )
    request["destination_versions"].append(
        {
            **request["destination_versions"][0],
            "version_id": "d2",
            "size": 5,
            "sha256": checksum(b"older"),
            "is_current": False,
        }
    )
    parsed = TrackerRequest.model_validate(request)
    assert parsed.plan is not None
    with pytest.raises(LifecycleConflict):
        await boundary.verify_objects(parsed, parsed.plan.runs[0])
