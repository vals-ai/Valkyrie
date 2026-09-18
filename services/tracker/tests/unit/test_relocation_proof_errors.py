"""Incomplete provider histories and invalid rewrites cannot authorize a location update."""

from typing import Any

import pytest

from tests.unit.test_relocation_providers import checksum, setup
from tracker.lifecycle import LifecycleConflict
from tracker.run_relocation.providers import rewrite_json
from tracker.storage_migration_exchange import JsonLocatorEdit, ObjectTransformation, TrackerRequest


@pytest.mark.parametrize(
    "content,pointer",
    [
        (b'{"url":"old","url":"other"}', "/url"),
        (b'{"url":NaN}', "/url"),
        (b'{"url":"changed"}', "/url"),
        (b'{"urls":["old"]}', "/urls/01"),
        (b'{"url":3}', "/url/missing"),
        (b'{"url":"old"}', "/missing"),
        (b"\xff", "/url"),
        (b'{"urls":[]}', "/urls/0"),
        (b'{"url":"old","n":1e999}', "/url"),
    ],
)
def test_rewrite_rejects_ambiguous_or_invalid_source_payload(content: bytes, pointer: str) -> None:
    transformation = ObjectTransformation(
        source_bucket="source",
        key="key",
        source_version_id="v1",
        original_size=len(content),
        original_sha256=checksum(content),
        rewritten_size=2,
        rewritten_sha256=checksum(b"{}"),
        edits=(JsonLocatorEdit(pointer=pointer, original="old", replacement="new"),),
    )
    with pytest.raises(LifecycleConflict):
        rewrite_json(content, transformation)


def test_nested_array_rewrite_preserves_exact_planned_bytes_and_rejects_wrong_result_digest() -> None:
    content = b'{"a/b":[{"~url":"old"}],"z":2}'
    expected = b'{"a/b":[{"~url":"new"}],"z":2}'
    transformation = ObjectTransformation(
        source_bucket="source",
        key="key",
        source_version_id="v1",
        original_size=len(content),
        original_sha256=checksum(content),
        rewritten_size=len(expected),
        rewritten_sha256=checksum(expected),
        edits=(JsonLocatorEdit(pointer="/a~1b/0/~0url", original="old", replacement="new"),),
    )
    assert rewrite_json(content, transformation) == expected
    with pytest.raises(LifecycleConflict, match="immutable plan"):
        rewrite_json(content, transformation.model_copy(update={"rewritten_sha256": "a" * 64}))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "version",
        "length",
        "identity",
        "size",
        "pagination-missing",
        "pagination-invalid",
        "pagination-cycle",
        "uploads",
        "current",
    ],
)
async def test_incomplete_provider_observations_cannot_verify_copy(failure: str) -> None:
    boundary, store, payload = setup()
    original_get, original_list = store.get_object, store.list_object_versions

    async def changed_get(**arguments: Any) -> dict[str, Any]:
        response = await original_get(**arguments)
        if failure == "version":
            response["VersionId"] = "another-version"
        elif failure == "length":
            response["ContentLength"] += 1
        return response

    async def changed_list(**arguments: Any) -> dict[str, Any]:
        response = await original_list(**arguments)
        if failure == "identity":
            response["Versions"][0]["Key"] = "outside-run"
        elif failure == "size":
            response["Versions"][0]["Size"] += 1
        elif failure == "pagination-missing":
            response.pop("IsTruncated")
        elif failure in {"pagination-invalid", "pagination-cycle"}:
            response["IsTruncated"] = True
            if failure == "pagination-cycle":
                response.update(NextKeyMarker="key", NextVersionIdMarker="version")
                if "KeyMarker" in arguments:
                    response["Versions"] = []
        elif failure == "current":
            response["Versions"][0]["IsLatest"] = False
        return response

    async def uploads(**_arguments: Any) -> dict[str, Any]:
        return {"Uploads": [{"Key": store.key}], "IsTruncated": False}

    store.get_object, store.list_object_versions = changed_get, changed_list
    if failure == "uploads":
        store.list_multipart_uploads = uploads
    request = TrackerRequest.model_validate(payload)
    assert request.plan is not None
    with pytest.raises(LifecycleConflict):
        await boundary.verify_objects(request, request.plan.runs[0])


@pytest.mark.asyncio
async def test_complete_paginated_versions_are_verified() -> None:
    boundary, store, payload = setup()
    original_list = store.list_object_versions

    async def paginated(**arguments: Any) -> dict[str, Any]:
        if "KeyMarker" not in arguments:
            return {
                "Versions": [],
                "DeleteMarkers": [],
                "IsTruncated": True,
                "NextKeyMarker": store.key,
                "NextVersionIdMarker": "cursor",
            }
        return await original_list(**arguments)

    store.list_object_versions = paginated
    request = TrackerRequest.model_validate(payload)
    assert request.plan is not None
    await boundary.verify_objects(request, request.plan.runs[0])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "duplicate-copy",
        "duplicate-history",
        "copy-bucket",
        "history-bucket",
        "marker-bytes",
        "missing-checksum",
        "source-bytes",
        "copy-current",
        "missing-transform",
        "copied-restoration",
        "unplanned-existing",
    ],
)
async def test_reviewed_history_must_match_every_copy_and_retained_version(failure: str) -> None:
    boundary, store, payload = setup()
    copy, history = payload["copied_objects"][0], payload["destination_versions"][0]
    if failure == "duplicate-copy":
        payload["copied_objects"].append(dict(copy))
    elif failure == "duplicate-history":
        payload["destination_versions"].append(dict(history))
    elif failure == "copy-bucket":
        copy["source_bucket"] = "foreign"
    elif failure == "history-bucket":
        history["bucket"] = "foreign"
    elif failure == "marker-bytes":
        history["is_delete_marker"] = True
    elif failure == "missing-checksum":
        history["sha256"] = None
    elif failure == "source-bytes":
        store.versions["source"] = [("s1", b"changed")]
    elif failure == "copy-current":
        copy["is_current"] = False
    elif failure == "missing-transform":
        copy["transformation_sha256"] = history["transformation_sha256"] = "b" * 64
    elif failure == "copied-restoration":
        history["restored_from_version_id"] = "d1"
    else:
        store.versions["source"] = []
        payload["copied_objects"] = []
        history.update(provenance="existing", restored_from_version_id="unexpected")
    request = TrackerRequest.model_validate(payload)
    assert request.plan is not None
    with pytest.raises(LifecycleConflict):
        await boundary.verify_objects(request, request.plan.runs[0])
