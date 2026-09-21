"""Exact provider cleanup boundaries."""

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from benchmark_service import SandboxNotFoundError
from botocore.exceptions import ClientError
from jsonschema import Draft202012Validator
from jsonschema import validate as validate_schema
from jsonschema.exceptions import ValidationError as SchemaValidationError
from pydantic import ValidationError

import tracker.run_purge.providers as providers
from tracker.aws.runtime import AWSResources
from tracker.lifecycle import LifecycleConflict, OperationIdentity, RunScope, unverified
from tracker.run_purge.contracts import ProviderLocator, PurgeRun
from tracker.run_purge.providers import AWSProviderBoundary, FenceReceipt, WriteProbe, policy_digest

Setup = tuple[AWSProviderBoundary, OperationIdentity, PurgeRun, Any, Any]


@pytest.fixture
def setup() -> Setup:
    run_id = uuid4()
    identity = OperationIdentity(
        operation_id=uuid4(),
        parent_plan_sha256="a" * 64,
        github_owner_id=42,
        org_id=uuid4(),
        source_aws_account_id="123456789012",
        destination_aws_account_id="123456789012",
        region="us-west-2",
        environment="dev",
        database_target="test",
        run_ids=(run_id,),
    )
    run = PurgeRun(
        scope=RunScope(
            run_id=run_id,
            original_resources=AWSResources(
                region="us-west-2", s3_bucket="vs-dev-owner-42", log_group="runs", log_retention_days=7
            ),
        ),
        provider=ProviderLocator(kind="daytona", secret_name="provider"),
    )
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ValSmithOwnerDeletion" + identity.operation_id.hex,
                "Effect": "Deny",
                "Principal": "*",
                "Action": ["s3:PutObject", "s3:DeleteObject"],
                "Resource": "arn:aws:s3:::vs-dev-owner-42/*",
            }
        ],
    }
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.head_bucket.return_value = {"BucketRegion": "us-west-2"}
    client.get_bucket_tagging.return_value = {
        "TagSet": [
            {"Key": key, "Value": value}
            for key, value in {
                "valsmith:owner-account-id": "42",
                "valsmith:environment": "dev",
                "valsmith:backup": "true",
                "valsmith:valkyrie-org-id": str(identity.org_id),
            }.items()
        ]
    }
    client.get_bucket_versioning.return_value = {"Status": "Enabled"}
    client.get_bucket_policy.return_value = {"Policy": json.dumps(policy)}
    client.list_object_versions.return_value = {"IsTruncated": False}
    client.list_multipart_uploads.return_value = {"IsTruncated": False}
    logs = MagicMock()
    logs.describe_log_groups.return_value = {"logGroups": []}
    clients = MagicMock()
    clients.sts_client.return_value.get_caller_identity.return_value = {"Account": "123456789012"}
    clients.credential_source = "managed"
    clients.with_region.return_value = clients
    clients.s3_client.return_value = client
    clients.cloudwatch_logs_client.return_value = logs
    receipt = FenceReceipt(
        identity=identity,
        bucket="vs-dev-owner-42",
        policy_sha256=policy_digest(policy),
        observed_at=datetime.now(UTC),
        write_probe=WriteProbe(
            key=f".valsmith-owner-deletion/{identity.operation_id}/write-probe", outcome="AccessDenied"
        ),
    )
    return AWSProviderBoundary(clients, fence_receipts=(receipt,)), identity, run, client, logs


@pytest.mark.asyncio
async def test_owner_mismatch_blocks_valid_named_bucket(setup: Setup) -> None:
    boundary, identity, run, client, _ = setup
    await boundary.validate(identity, run)
    client.get_bucket_tagging.return_value["TagSet"][0]["Value"] = "43"
    with pytest.raises((LifecycleConflict, ValueError)):
        await boundary.validate(identity, run)


@pytest.mark.asyncio
async def test_current_fence_exact_statement_and_full_hash(setup: Setup) -> None:
    boundary, identity, run, client, _ = setup
    await boundary.verify_fence(identity, run)
    policy = json.loads(client.get_bucket_policy.return_value["Policy"])
    policy["Statement"][0]["Condition"] = {"Bool": {"aws:SecureTransport": "false"}}
    client.get_bucket_policy.return_value = {"Policy": json.dumps(policy)}
    with pytest.raises(LifecycleConflict):
        await boundary.verify_fence(identity, run)


@pytest.mark.asyncio
async def test_all_versions_markers_uploads_and_exact_logs(setup: Setup) -> None:
    boundary, identity, run, client, logs = setup
    await boundary.verify_fence(identity, run)
    statement = json.loads(client.get_bucket_policy.return_value["Policy"])["Statement"][0]
    schema = json.loads((Path(__file__).parents[4] / "docs/deployment/tracker-purge.schema.json").read_text())
    validate_schema(statement, schema["OwnerDeletionFence"], cls=Draft202012Validator)
    for invalid_actions in ("s3:PutObject", [*statement["Action"], "s3:DeleteObjectVersion"]):
        with pytest.raises(SchemaValidationError):
            validate_schema(
                {**statement, "Action": invalid_actions}, schema["OwnerDeletionFence"], cls=Draft202012Validator
            )

    async def delete_with_policy(**arguments: Any) -> None:
        action = "s3:DeleteObjectVersion" if "VersionId" in arguments else "s3:DeleteObject"
        if action in statement["Action"]:
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "DeleteObject")

    client.delete_object.side_effect = delete_with_policy
    prefix = run.scope.object_prefix
    with pytest.raises(ClientError, match="AccessDenied"):
        await client.delete_object(Bucket=run.scope.original_resources.s3_bucket, Key=prefix + "delayed-delete")
    client.delete_object.reset_mock()
    client.list_object_versions.side_effect = [
        {
            "IsTruncated": False,
            "Versions": [{"Key": prefix + "a", "VersionId": "v1"}],
            "DeleteMarkers": [{"Key": prefix + "a", "VersionId": "v2"}],
        },
        {"IsTruncated": False},
    ]
    client.list_multipart_uploads.side_effect = [
        {"IsTruncated": False, "Uploads": [{"Key": prefix + "b", "UploadId": "u1"}]},
        {"IsTruncated": False},
    ]
    await boundary.purge_objects(identity, run, verify=unverified)
    deleted = {(call.kwargs["Key"], call.kwargs["VersionId"]) for call in client.delete_object.call_args_list}
    assert deleted == {(prefix + "a", "v1"), (prefix + "a", "v2")}
    assert client.abort_multipart_upload.call_args.kwargs["UploadId"] == "u1"
    logs.describe_log_groups.side_effect = [
        {"logGroups": [{"logGroupName": run.scope.log_group}, {"logGroupName": run.scope.log_group + "-other"}]},
        {"logGroups": [{"logGroupName": run.scope.log_group + "-other"}]},
    ]
    await boundary.purge_logs(run, verify=unverified)
    logs.delete_log_group.assert_called_once_with(logGroupName=run.scope.log_group)


@pytest.mark.asyncio
async def test_provider_listing_outside_prefix_refuses_delete(setup: Setup) -> None:
    boundary, identity, run, client, _ = setup
    client.list_object_versions.return_value = {"Versions": [{"Key": "benchmarks/another/a", "VersionId": "v1"}]}
    with pytest.raises(LifecycleConflict):
        await boundary.purge_objects(identity, run, verify=unverified)
    client.delete_object.assert_not_called()


@pytest.mark.asyncio
async def test_two_run_storage_keeps_unrelated_versions_and_logs(setup: Setup) -> None:
    boundary, identity, run, client, logs = setup
    other_prefix = f"benchmarks/{uuid4()}/"
    versions = {
        (run.scope.object_prefix + "data", "old"),
        (run.scope.object_prefix + "data", "current"),
        (other_prefix + "data", "keep"),
    }
    markers = {(run.scope.object_prefix + "gone", "marker"), (other_prefix + "gone", "keep-marker")}
    uploads = {(run.scope.object_prefix + "upload", "inflight"), (other_prefix + "upload", "keep-upload")}
    groups = {run.scope.log_group, "runs/shared", run.scope.log_group + "-unrelated"}

    async def list_versions(**arguments: Any) -> dict[str, Any]:
        assert arguments["ExpectedBucketOwner"] == identity.source_aws_account_id
        prefix = arguments["Prefix"]
        return {
            "IsTruncated": False,
            "Versions": [{"Key": key, "VersionId": version} for key, version in versions if key.startswith(prefix)],
            "DeleteMarkers": [{"Key": key, "VersionId": version} for key, version in markers if key.startswith(prefix)],
        }

    async def list_uploads(**arguments: Any) -> dict[str, Any]:
        return {
            "IsTruncated": False,
            "Uploads": [
                {"Key": key, "UploadId": upload} for key, upload in uploads if key.startswith(arguments["Prefix"])
            ],
        }

    async def remove_version(**arguments: Any) -> None:
        item = (arguments["Key"], arguments["VersionId"])
        versions.discard(item)
        markers.discard(item)

    async def abort(**arguments: Any) -> None:
        uploads.remove((arguments["Key"], arguments["UploadId"]))

    client.list_object_versions.side_effect = list_versions
    client.list_multipart_uploads.side_effect = list_uploads
    client.delete_object.side_effect = remove_version
    client.abort_multipart_upload.side_effect = abort

    def list_groups(**_arguments: Any) -> dict[str, Any]:
        return {"logGroups": [{"logGroupName": group} for group in groups]}

    def delete_group(**arguments: Any) -> None:
        groups.remove(arguments["logGroupName"])

    logs.describe_log_groups.side_effect = list_groups
    logs.delete_log_group.side_effect = delete_group
    await boundary.purge_objects(identity, run, verify=unverified)
    await boundary.purge_logs(run, verify=unverified)
    await boundary.verify_storage_absence(identity, run)
    assert versions == {(other_prefix + "data", "keep")}
    assert markers == {(other_prefix + "gone", "keep-marker")}
    assert uploads == {(other_prefix + "upload", "keep-upload")}
    assert groups == {"runs/shared", run.scope.log_group + "-unrelated"}


@pytest.mark.asyncio
async def test_strict_sandbox_delete_error_is_not_swallowed(setup: Setup, monkeypatch: pytest.MonkeyPatch) -> None:

    boundary, _, run, _, _ = setup
    sandbox = MagicMock()
    sandbox.id = "sandbox"
    sandbox.labels = {"Id": str(run.scope.run_id)}

    async def inventory(_query: Any) -> AsyncIterator[Any]:
        yield sandbox

    provider = MagicMock()
    provider.list_sandboxes = inventory
    provider.delete_sandbox = AsyncMock(side_effect=RuntimeError("unexpected provider failure"))
    provider.close = AsyncMock()
    configuration = MagicMock()
    configuration.create_provider.return_value = provider

    def configuration_lookup(*_arguments: Any) -> Any:
        return configuration

    monkeypatch.setattr(providers, "fetch_sandbox_provider_config", configuration_lookup)
    with pytest.raises(RuntimeError, match="unexpected provider"):
        await boundary.cleanup_sandboxes(run, verify=unverified)
    assert provider.close.await_count == 1


def test_write_fence_receipt_rejects_wrong_operation_probe(setup: Setup) -> None:
    boundary, _, _, _, _ = setup
    document = boundary.fence_receipts[0].model_dump(mode="json")
    document["write_probe"] = {"key": f".valsmith-owner-deletion/{uuid4()}/write-probe", "outcome": "AccessDenied"}
    with pytest.raises(ValidationError):
        FenceReceipt.model_validate(document)


def test_fence_receipt_accepts_only_operation_bound_denied_probe(setup: Setup) -> None:
    boundary, identity, _, _, _ = setup
    document = boundary.fence_receipts[0].model_dump(mode="json")
    document.update(
        observed_at=datetime.now(UTC).isoformat(),
        write_probe={"key": f".valsmith-owner-deletion/{identity.operation_id}/write-probe", "outcome": "AccessDenied"},
    )
    receipt = FenceReceipt.model_validate(document)
    assert receipt.write_probe.outcome == "AccessDenied"
    document["write_probe"]["outcome"] = "unknown"
    with pytest.raises(ValidationError):
        FenceReceipt.model_validate(document)


@pytest.mark.asyncio
async def test_missing_pagination_proof_cannot_count_as_empty_storage(setup: Setup) -> None:
    boundary, identity, run, client, _ = setup
    client.list_object_versions.return_value = {}
    with pytest.raises(LifecycleConflict, match="pagination"):
        await boundary.verify_storage_absence(identity, run)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "actions, accepted",
    [
        ("s3:PutObject", False),
        (["s3:PutObject", "s3:DeleteObject"], True),
        (["s3:PutObject", "s3:DeleteObject", "s3:DeleteObjectVersion"], False),
    ],
)
async def test_fence_blocks_marker_creation_but_permits_version_cleanup(
    setup: Setup, actions: Any, accepted: bool
) -> None:
    boundary, identity, run, client, _ = setup
    policy = json.loads(client.get_bucket_policy.return_value["Policy"])
    policy["Statement"][0]["Action"] = actions
    client.get_bucket_policy.return_value = {"Policy": json.dumps(policy)}
    boundary.fence_receipts = (boundary.fence_receipts[0].model_copy(update={"policy_sha256": policy_digest(policy)}),)
    if accepted:
        await boundary.verify_fence(identity, run)
        await boundary.purge_objects(identity, run, verify=unverified)
    else:
        with pytest.raises(LifecycleConflict):
            await boundary.verify_fence(identity, run)
        client.delete_object.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["account", "owner", "missing_receipt", "future_receipt"])
async def test_authority_failures_prevent_storage_mutation(setup: Setup, mismatch: str) -> None:
    boundary, identity, run, client, _ = setup
    if mismatch == "account":
        clients = cast(Any, boundary.clients)
        clients.sts_client.return_value.get_caller_identity.return_value = {"Account": "999999999999"}
    elif mismatch == "owner":
        identity = identity.model_copy(update={"github_owner_id": 43})
    elif mismatch == "missing_receipt":
        boundary.fence_receipts = ()
    else:
        boundary.fence_receipts = (
            boundary.fence_receipts[0].model_copy(update={"observed_at": datetime.now(UTC) + timedelta(minutes=1)}),
        )
    with pytest.raises(LifecycleConflict):
        if mismatch in {"account", "owner"}:
            await boundary.validate(identity, run)
        else:
            await boundary.verify_fence(identity, run)
    client.delete_object.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("uploads", [False, True])
@pytest.mark.parametrize("broken", [False, True])
async def test_inventory_pages_all_items_or_refuses_repeated_markers(setup: Setup, uploads: bool, broken: bool) -> None:
    boundary, identity, run, client, _ = setup
    key = run.scope.object_prefix + "payload"
    collection, identifier, next_marker = (
        ("Uploads", "UploadId", "NextUploadIdMarker") if uploads else ("Versions", "VersionId", "NextVersionIdMarker")
    )
    first = {
        "IsTruncated": True,
        "NextKeyMarker": key,
        next_marker: "first",
        collection: [{"Key": key, identifier: "first"}],
    }
    second = first if broken else {"IsTruncated": False, collection: [{"Key": key, identifier: "second"}]}
    listing = client.list_multipart_uploads if uploads else client.list_object_versions
    listing.side_effect = [first, second, {"IsTruncated": False}]
    if broken:
        with pytest.raises(LifecycleConflict, match="pagination"):
            await boundary.purge_objects(identity, run, verify=unverified)
        client.delete_object.assert_not_called()
        client.abort_multipart_upload.assert_not_called()
    else:
        await boundary.purge_objects(identity, run, verify=unverified)
        deletions = client.abort_multipart_upload if uploads else client.delete_object
        assert {call.kwargs[identifier] for call in deletions.call_args_list} == {"first", "second"}
        assert listing.call_args_list[1].kwargs["KeyMarker"] == key


@pytest.mark.asyncio
@pytest.mark.parametrize("error_code", ["NoSuchUpload", "AccessDenied"])
async def test_multipart_race_requires_verified_absence(setup: Setup, error_code: str) -> None:
    boundary, identity, run, client, _ = setup
    client.list_multipart_uploads.side_effect = [
        {"IsTruncated": False, "Uploads": [{"Key": run.scope.object_prefix + "data", "UploadId": "upload"}]},
        {"IsTruncated": False},
    ]
    client.abort_multipart_upload.side_effect = ClientError({"Error": {"Code": error_code}}, "AbortMultipartUpload")
    if error_code == "NoSuchUpload":
        await boundary.purge_objects(identity, run, verify=unverified)
        assert client.list_multipart_uploads.call_count == 2
    else:
        with pytest.raises(ClientError):
            await boundary.purge_objects(identity, run, verify=unverified)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["repeated_token", "access_denied", "disappeared", "retained"])
async def test_log_pagination_and_delete_races_are_strict(setup: Setup, failure: str) -> None:
    boundary, _, run, _, logs = setup
    page = {"logGroups": [{"logGroupName": run.scope.log_group + "-other"}], "nextToken": "next"}
    target = {"logGroups": [{"logGroupName": run.scope.log_group}]}
    logs.describe_log_groups.side_effect = [
        page,
        page if failure == "repeated_token" else target,
        target if failure == "retained" else {"logGroups": []},
    ]
    if failure in {"access_denied", "disappeared"}:
        code = "AccessDeniedException" if failure == "access_denied" else "ResourceNotFoundException"
        logs.delete_log_group.side_effect = ClientError({"Error": {"Code": code}}, "DeleteLogGroup")
    if failure == "disappeared":
        await boundary.purge_logs(run, verify=unverified)
        assert logs.describe_log_groups.call_count == 3
    else:
        with pytest.raises((LifecycleConflict, ClientError)):
            await boundary.purge_logs(run, verify=unverified)
    if failure == "repeated_token":
        logs.delete_log_group.assert_not_called()
    else:
        logs.delete_log_group.assert_called_once_with(logGroupName=run.scope.log_group)


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["absent", "remains", "wrong_run", "delete_not_found"])
async def test_sandbox_inventory_scope_and_not_found_cleanup(
    setup: Setup, monkeypatch: pytest.MonkeyPatch, scenario: str
) -> None:
    boundary, _, run, _, _ = setup
    sandbox = MagicMock(id="sandbox", labels={"Id": str(uuid4() if scenario == "wrong_run" else run.scope.run_id)})

    async def inventory(query: Any) -> AsyncIterator[Any]:
        assert query.labels == {"Id": str(run.scope.run_id)}
        if scenario != "absent":
            yield sandbox

    provider = MagicMock()
    provider.list_sandboxes = inventory
    provider.delete_sandbox = AsyncMock(side_effect=SandboxNotFoundError("gone"))
    provider.close = AsyncMock()
    configuration = MagicMock()
    configuration.create_provider.return_value = provider

    def lookup(*_arguments: Any) -> Any:
        return configuration

    monkeypatch.setattr(providers, "fetch_sandbox_provider_config", lookup)
    if scenario == "delete_not_found":
        await boundary.cleanup_sandboxes(run, verify=unverified)
    elif scenario == "absent":
        await boundary.verify_absence(run)
    else:
        with pytest.raises(LifecycleConflict):
            await boundary.verify_absence(run)
        provider.delete_sandbox.assert_not_called()
    provider.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("remaining", ["version", "upload", "logs"])
async def test_final_storage_absence_rejects_any_remaining_resource(setup: Setup, remaining: str) -> None:
    boundary, identity, run, client, logs = setup
    if remaining == "version":
        client.list_object_versions.return_value = {
            "IsTruncated": False,
            "Versions": [{"Key": run.scope.object_prefix + "data", "VersionId": "version"}],
        }
    elif remaining == "upload":
        client.list_multipart_uploads.return_value = {
            "IsTruncated": False,
            "Uploads": [{"Key": run.scope.object_prefix + "data", "UploadId": "upload"}],
        }
    else:
        logs.describe_log_groups.return_value = {"logGroups": [{"logGroupName": run.scope.log_group}]}
    with pytest.raises(LifecycleConflict):
        await boundary.verify_storage_absence(identity, run)
