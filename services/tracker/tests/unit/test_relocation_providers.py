"""Exact version history and JSON transform verification at the provider boundary."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock
from uuid import uuid4

import pytest

from tests.relocation_support import VersionStore
from tracker.aws.runtime import AWSResources
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope
from tracker.run_purge.contracts import ProviderLocator, PurgeRun
from tracker.run_relocation import retired_source_buckets
from tracker.run_relocation.providers import CHUNK_BYTES, RelocationAWSBoundary, rewrite_json
from tracker.storage_migration_exchange import JsonLocatorEdit, ObjectTransformation, TrackerRequest, canonical_digest


def checksum(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


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
@pytest.mark.parametrize("change", ["absent", "put_only", "condition", "other_scope", "other_bucket"])
async def test_copy_verification_requires_current_exact_source_fence(change: str) -> None:
    boundary, store, request = setup()
    assert store.fence_statement is not None
    if change == "absent":
        store.fence_statement = None
    elif change == "put_only":
        store.fence_statement["Action"] = "s3:PutObject"
    elif change == "condition":
        store.fence_statement["Condition"] = {"Bool": {"aws:SecureTransport": "false"}}
    elif change == "other_bucket":
        store.fence_statement["Resource"] = [*store.fence_statement["Resource"], "arn:aws:s3:::destination/*"]
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["foreign_owner", "managed_name", "foreign_org", "unknown_versioning", "wrong_region", "wrong_account"]
)
async def test_source_authority_never_falls_back_from_foreign_managed_scope(change: str) -> None:

    boundary, store, request = setup()
    assert request["plan"] is not None
    identity = OperationIdentity.model_validate(request["plan"]["identity"])
    bucket = "vs-dev-other-99" if change == "managed_name" else "source"
    resources = AWSResources("us-east-1", bucket, "runs", 7)
    run = PurgeRun(
        scope=RunScope(run_id=identity.run_ids[0], original_resources=resources),
        provider=ProviderLocator(kind="daytona", secret_name="provider"),
    )
    clients = cast(Mock, boundary.clients)
    clients.credential_source = "managed"
    clients.sts_client.return_value.get_caller_identity.return_value = {"Account": "123456789012"}
    store.tags[bucket] = []
    if change == "foreign_owner":
        store.tags[bucket] = [{"Key": "valsmith:owner-account-id", "Value": "99"}]
    elif change == "foreign_org":
        store.tags[bucket] = [{"Key": "valsmith:valkyrie-org-id", "Value": str(uuid4())}]
    elif change == "unknown_versioning":
        store.versioning[bucket] = {"Status": "Unknown"}
    elif change == "wrong_region":
        store.region = "us-west-2"
    elif change == "wrong_account":
        clients.sts_client.return_value.get_caller_identity.return_value = {"Account": "999999999999"}
    with pytest.raises(LifecycleConflict):
        await boundary.validate_source(identity, run)


@pytest.mark.asyncio
@pytest.mark.parametrize("historical_marker", [False, True])
async def test_tied_source_history_cannot_prove_copy_order(historical_marker: bool) -> None:
    boundary, store, request = setup()
    historical = None if historical_marker else b"older2"
    store.versions["source"] += [("s2", historical), ("s3", b"older3")]
    store.versions["destination"] += [("d3", b"older3"), ("d2", historical)]
    original_list = store.list_object_versions

    async def tied_source(**arguments: Any) -> dict[str, Any]:
        result = await original_list(**arguments)
        if arguments["Bucket"] == "source":
            for item in result["Versions"] + result["DeleteMarkers"]:
                if not item["IsLatest"]:
                    item["LastModified"] = datetime(2025, 1, 1, tzinfo=UTC)
        return result

    store.list_object_versions = tied_source
    for number, content in [(3, b"older3"), (2, historical)]:
        size = len(content or b"")
        content_digest = None if content is None else checksum(content)
        request["copied_objects"].append(
            {
                **request["copied_objects"][0],
                "source_version_id": f"s{number}",
                "destination_version_id": f"d{number}",
                "source_size": size,
                "destination_size": size,
                "source_sha256": content_digest,
                "destination_sha256": content_digest,
                "is_current": False,
                "is_delete_marker": content is None,
            }
        )
        request["destination_versions"].append(
            {
                **request["destination_versions"][0],
                "version_id": f"d{number}",
                "size": size,
                "sha256": content_digest,
                "is_current": False,
                "is_delete_marker": content is None,
            }
        )
    parsed = TrackerRequest.model_validate(request)
    assert parsed.plan is not None
    with pytest.raises(LifecycleConflict, match="ambiguous"):
        await boundary.verify_objects(parsed, parsed.plan.runs[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("identifier", [None, "null"])
async def test_unversioned_retained_reference_is_unknown_and_cannot_claim_an_immutable_version(
    identifier: str | None,
) -> None:
    boundary, store, payload = setup()
    store.execution_objects["retained", "manifest.json"] = (identifier, b"{}")
    request = TrackerRequest.model_validate(payload)
    references = await boundary.execution_references({"dataset": "s3://retained/manifest.json"}, request, frozenset())
    assert len(references) == 1
    assert references[0].kind == "unknown" and references[0].version_id is None
    with pytest.raises(LifecycleConflict, match="another version"):
        await boundary.execution_references(
            {"dataset": "s3://retained/manifest.json?versionId=immutable"}, request, frozenset()
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "suffix",
    [
        "",
        "?versionId=",
        "?versionId=null",
        "?versionId=v1&versionId=",
        "?versionId=v1&versionId=v2",
        "?other=v1",
        "?versionId=v1#fragment",
    ],
)
async def test_unpinned_saved_s3_locator_is_unknown_without_fetching_current_object(suffix: str) -> None:
    boundary, store, payload = setup()

    async def unavailable_object(**_arguments: Any) -> dict[str, Any]:
        raise AssertionError("Unpinned locator must not fetch the mutable current object")

    store.get_object = unavailable_object
    (reference,) = await boundary.execution_references(
        {"dataset": "s3://retained/manifest.json" + suffix}, TrackerRequest.model_validate(payload), frozenset()
    )
    assert reference.kind == "unknown" and reference.version_id is None


@pytest.mark.asyncio
async def test_saved_immutable_s3_locator_verifies_exact_version_bytes() -> None:
    boundary, store, payload = setup()
    store.execution_objects["retained", "manifest.json"] = ("v1", b"{}")
    (reference,) = await boundary.execution_references(
        {"dataset": "s3://retained/manifest.json?versionId=v1"}, TrackerRequest.model_validate(payload), frozenset()
    )
    assert (reference.kind, reference.version_id, reference.sha256) == ("retained_s3_object", "v1", checksum(b"{}"))


@pytest.mark.asyncio
async def test_retained_reference_bytes_are_hashed_in_bounded_chunks() -> None:
    boundary, store, payload = setup()
    content = b"retained" * (CHUNK_BYTES // 4)
    store.execution_objects["retained", "manifest.json"] = ("v1", content)
    locator = {"dataset": "s3://retained/manifest.json?versionId=v1"}

    (reference,) = await boundary.execution_references(locator, TrackerRequest.model_validate(payload), frozenset())

    assert reference.sha256 == checksum(content)
    assert store.streams and all(amount == CHUNK_BYTES for stream in store.streams for amount in stream.reads)

    async def truncated_object(**_arguments: Any) -> dict[str, Any]:
        return {"Body": store.stream(b"{}"), "ContentLength": len(content), "VersionId": "v1"}

    store.get_object = truncated_object
    with pytest.raises(LifecycleConflict, match="incomplete"):
        await boundary.execution_references(locator, TrackerRequest.model_validate(payload), frozenset())


@pytest.mark.asyncio
async def test_an_under_declared_rewrite_source_is_refused_before_its_body_is_materialized() -> None:
    boundary, store, payload = setup()
    declared = len(b'{"value":1}')

    async def under_declared(**_arguments: Any) -> dict[str, Any]:
        return {"Body": store.stream(b"x" * CHUNK_BYTES * 2), "ContentLength": declared, "VersionId": "s1"}

    store.get_object = under_declared

    with pytest.raises(LifecycleConflict, match="does not match exact version"):
        await boundary._bytes(store, TrackerRequest.model_validate(payload), "source", store.key, "s1")

    (stream,) = store.streams
    assert None not in stream.reads
    assert stream.position <= declared + 1


def two_bucket_plan(payload: dict[str, Any], second_bucket: str = "other-source") -> list[dict[str, Any]]:
    first: dict[str, Any] = payload["plan"]["runs"][0]
    second_id = str(uuid4())
    second: dict[str, Any] = {
        "scope": {
            "run_id": second_id,
            "original_resources": {**first["scope"]["original_resources"], "s3_bucket": second_bucket},
            "object_prefix": f"benchmarks/{second_id}/",
            "log_group": f"runs/{second_id}",
        },
        "destination_resources": dict(first["destination_resources"]),
        "expected_label": None,
        "execution_policy": "history_only",
        "execution_arguments_sha256": "d" * 64,
    }
    runs: list[dict[str, Any]] = sorted([first, second], key=lambda item: str(item["scope"]["run_id"]))
    run_ids = [item["scope"]["run_id"] for item in runs]
    payload["plan"]["runs"] = runs
    payload["plan"]["identity"]["run_ids"] = run_ids
    payload["run_ids"] = run_ids
    return runs


@pytest.mark.asyncio
async def test_a_shared_source_fence_holds_every_planned_prefix_and_nothing_outside_the_bucket() -> None:
    boundary, store, payload = setup()
    proved_run_id = payload["copied_objects"][0]["run_id"]
    runs = two_bucket_plan(payload, "source")
    request = TrackerRequest.model_validate(payload)
    assert request.plan is not None and store.fence_statement is not None
    run = next(item for item in request.plan.runs if str(item.scope.run_id) == proved_run_id)
    prefixes = [f"arn:aws:s3:::source/{item['scope']['object_prefix']}*" for item in runs]

    store.fence_statement["Resource"] = prefixes
    await boundary.verify_objects(request, run)

    # The parent fences its own wider plan too, including exact keys that carry no wildcard.
    store.fence_statement["Resource"] = [*prefixes, "arn:aws:s3:::source/shared/manifest.json", "arn:aws:s3:::source/*"]
    await boundary.verify_objects(request, run)

    store.fence_statement["Resource"] = [f"arn:aws:s3:::source/{run.scope.object_prefix}*"]
    with pytest.raises(LifecycleConflict, match="exact operation scope"):
        await boundary.verify_objects(request, run)

    store.fence_statement["Resource"] = [*prefixes, "arn:aws:s3:::other-source/shared/manifest.json"]
    with pytest.raises(LifecycleConflict, match="exact operation scope"):
        await boundary.verify_objects(request, run)


@pytest.mark.asyncio
async def test_every_plan_source_bucket_is_retired_in_inventory_and_release_alike() -> None:
    boundary, store, payload = setup()
    runs = two_bucket_plan(payload)
    request = TrackerRequest.model_validate(payload)
    saved = [str(item["scope"]["original_resources"]["s3_bucket"]) for item in runs]

    inventory_buckets = retired_source_buckets(request.model_copy(update={"plan": None}), saved)
    release_buckets = retired_source_buckets(request)
    assert inventory_buckets == release_buckets == frozenset({"source", "other-source"})

    store.execution_objects["source", store.key] = ("s1", b'{"value":1}')
    store.execution_objects["other-source", "manifest.json"] = ("v1", b"{}")
    for retired in (inventory_buckets, release_buckets):
        own = await boundary.execution_references(
            {"dataset": f"s3://source/{store.key}?versionId=s1", "contract": {}}, request, retired
        )
        other = await boundary.execution_references(
            {"dataset": "s3://other-source/manifest.json?versionId=v1", "contract": {}}, request, retired
        )
        assert [item.kind for item in own] == ["retired_source"]
        assert [item.kind for item in other] == ["retired_source"]


@pytest.mark.asyncio
async def test_version_proof_streams_bounded_chunks_and_reuses_proved_versions_after_commit() -> None:
    boundary, store, payload = setup()
    request = TrackerRequest.model_validate(payload)
    assert request.plan is not None

    await boundary.verify_objects(request, request.plan.runs[0])
    assert store.body_fetches == 2
    assert store.streams and all(amount == CHUNK_BYTES for stream in store.streams for amount in stream.reads)

    await boundary.verify_objects(request, request.plan.runs[0], source_partial=True, reuse_verified=True)
    assert store.body_fetches == 2

    store.versions["destination"] = [("replaced", b'{"value":1}')]
    payload["destination_versions"][0]["version_id"] = "replaced"
    payload["copied_objects"][0]["destination_version_id"] = "replaced"
    await boundary.verify_objects(
        TrackerRequest.model_validate(payload), request.plan.runs[0], source_partial=True, reuse_verified=True
    )
    assert store.body_fetches == 3


@pytest.mark.asyncio
async def test_hold_only_proves_destination_history_without_source_copy_or_cleanup() -> None:
    boundary, store, payload = setup()
    planned = payload["plan"]["runs"][0]
    planned["scope"]["original_resources"]["s3_bucket"] = "destination"
    planned["location_policy"] = "hold_only"
    payload["copied_objects"] = []
    payload["destination_versions"][0]["provenance"] = "existing"
    store.fence_statement = None
    parsed = TrackerRequest.model_validate(payload)
    assert parsed.plan is not None
    await boundary.verify_objects(parsed, parsed.plan.runs[0], source_removed=True)
    store.versions["destination"].append(("late", b"new"))
    with pytest.raises(LifecycleConflict, match="history"):
        await boundary.verify_objects(parsed, parsed.plan.runs[0], source_removed=True)


@pytest.mark.asyncio
async def test_hold_only_retained_execution_reference_is_not_retired_source() -> None:
    boundary, store, payload = setup()
    run = payload["plan"]["runs"][0]
    run["location_policy"] = "hold_only"
    run["scope"]["original_resources"]["s3_bucket"] = "destination"
    store.execution_objects["destination", "manifest.json"] = ("pinned", b"{}")
    request = TrackerRequest.model_validate(payload)
    assert request.plan is not None
    references = await boundary.execution_references(
        {"dataset": "s3://destination/manifest.json?versionId=pinned"}, request, retired_source_buckets(request)
    )
    assert references[0].kind == "retained_s3_object"
    assert references[0].version_id == "pinned"
    payload["copied_objects"][0]["source_bucket"] = "destination"
    parsed = TrackerRequest.model_validate(payload)
    with pytest.raises(LifecycleConflict, match="cannot claim copied"):
        await boundary.verify_objects(parsed, request.plan.runs[0])


@pytest.mark.asyncio
async def test_hold_only_checks_restored_manifest_edits_against_retained_original() -> None:
    boundary, store, payload = setup()
    planned = payload["plan"]["runs"][0]
    planned["location_policy"] = "hold_only"
    planned["scope"]["original_resources"]["s3_bucket"] = "destination"
    original = b'{"uri":"s3://legacy/object"}'
    rewritten = b'{"uri":"s3://destination/object"}'
    transformation = ObjectTransformation(
        source_bucket="destination",
        key=store.key,
        source_version_id="original",
        original_size=len(original),
        original_sha256=checksum(original),
        rewritten_size=len(rewritten),
        rewritten_sha256=checksum(rewritten),
        edits=(JsonLocatorEdit(pointer="/uri", original="s3://legacy/object", replacement="s3://destination/object"),),
    )
    planned["transformations"] = [transformation.model_dump(mode="json")]
    payload["copied_objects"] = []
    payload["destination_versions"] = [
        {
            "run_id": payload["run_ids"][0],
            "bucket": "destination",
            "key": store.key,
            "version_id": version,
            "is_delete_marker": False,
            "size": len(content),
            "sha256": checksum(content),
            "is_current": version == "restored",
            "provenance": "restored" if version == "restored" else "existing",
            "restored_from_version_id": "original" if version == "restored" else None,
            "transformation_sha256": canonical_digest(transformation.model_dump(mode="json"))
            if version == "restored"
            else None,
        }
        for version, content in (("restored", rewritten), ("original", original))
    ]
    store.versions["destination"] = [("restored", rewritten), ("original", original)]
    request = TrackerRequest.model_validate(payload)
    assert request.plan is not None
    await boundary.verify_objects(request, request.plan.runs[0], source_removed=True)
    store.versions["destination"] = [("restored", rewritten)]
    with pytest.raises(LifecycleConflict, match="history"):
        await boundary.verify_objects(request, request.plan.runs[0], source_removed=True)
