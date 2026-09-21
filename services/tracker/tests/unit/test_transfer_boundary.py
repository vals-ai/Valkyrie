"""Paired provider authority and final source log cleanup use current evidence."""

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, call

import pytest

from tests.transfer_support import ObservedEventsBoundary
from tests.unit.aws.test_log_history_archive import FakeLogs, FakeS3, FakeSession
from tracker.lifecycle import LifecycleConflict
from tracker.run_transfer.contracts import TransferRequest
from tracker.run_transfer.providers import SOURCE_FENCE_ACTIONS, ProfileClients, TransferAWSBoundary


def request_fixture() -> TransferRequest:
    return TransferRequest.model_validate_json(
        (Path(__file__).parents[1] / "fixtures/tracker-transfer-plan-v1.json").read_bytes()
    )


# Named here, not imported, so narrowing the deployed fence fails this test.
WIDENED_FENCE_ACTIONS = (
    "s3:AbortMultipartUpload",
    "s3:DeleteObjectTagging",
    "s3:DeleteObjectVersionTagging",
    "s3:PutObjectTagging",
    "s3:PutObjectVersionTagging",
    "s3:ReplicateDelete",
    "s3:ReplicateObject",
    "s3:ReplicateTags",
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        None,
        "account",
        "missing_fence",
        "fence_condition",
        "fence_prefix",
        "legacy_fence",
        "version_deletion_denied",
        *WIDENED_FENCE_ACTIONS,
    ],
)
async def test_import_requires_separate_accounts_and_exact_source_fence(tmp_path: Path, fault: str | None) -> None:
    payload = request_fixture().model_dump(mode="json")
    payload["action"] = "import"
    payload["plan"]["runs"][0]["destination"]["original_resources"]["s3_bucket"] = "vs-prod-owner-42"
    request = TransferRequest.model_validate(payload)
    run = request.plan.runs[0]
    clients: list[Any] = []
    storage: list[Any] = []
    for identity, scope, account in (
        (request.plan.source_identity, run.source, request.plan.source_identity.source_aws_account_id),
        (
            request.plan.destination_identity,
            run.destination,
            request.plan.destination_identity.destination_aws_account_id,
        ),
    ):
        client = AsyncMock()
        client.__aenter__.return_value = client
        client.head_bucket.return_value = {"BucketRegion": identity.region}
        client.get_bucket_versioning.return_value = {"Status": "Enabled"}
        client.get_bucket_tagging.return_value = {
            "TagSet": [
                {"Key": key, "Value": value}
                for key, value in {
                    "valsmith:owner-account-id": str(identity.github_owner_id),
                    "valsmith:environment": identity.environment,
                    "valsmith:backup": "true",
                    "valsmith:valkyrie-org-id": str(identity.org_id),
                }.items()
            ]
        }
        if scope == run.source:
            client.get_bucket_tagging.return_value = {"TagSet": []}
        authority = Mock()
        authority.credential_source = "managed"
        authority.with_region.return_value = authority
        authority.sts_client.return_value.get_caller_identity.return_value = {"Account": account}
        authority.s3_client.return_value = client
        clients.append(authority)
        storage.append(client)
    fence: dict[str, Any] = {
        "Sid": "ValSmithOwnerMigration" + request.plan.source_identity.operation_id.hex,
        "Effect": "Deny",
        "Principal": "*",
        "Action": list(SOURCE_FENCE_ACTIONS),
        "Resource": [f"arn:aws:s3:::{run.source.original_resources.s3_bucket}/{run.source.object_prefix}*"],
    }
    if fault in WIDENED_FENCE_ACTIONS:
        fence["Action"] = [action for action in SOURCE_FENCE_ACTIONS if action != fault]
    elif fault == "legacy_fence":
        fence["Action"] = ["s3:PutObject", "s3:DeleteObject"]
    elif fault == "version_deletion_denied":
        fence["Action"] = [*SOURCE_FENCE_ACTIONS, "s3:DeleteObjectVersion"]
    elif fault == "account":
        clients[1].sts_client.return_value.get_caller_identity.return_value = {
            "Account": request.plan.source_identity.source_aws_account_id
        }
    elif fault == "fence_condition":
        fence["Condition"] = {"Bool": {"aws:SecureTransport": "false"}}
    elif fault == "fence_prefix":
        fence["Resource"] = ["arn:aws:s3:::other/*"]
    storage[0].get_bucket_policy.return_value = {
        "Policy": json.dumps({"Statement": [] if fault == "missing_fence" else [fence]})
    }
    boundary = TransferAWSBoundary(clients[0], clients[1], tmp_path)
    if fault is None:
        await boundary.validate(request, run)
        assert (
            storage[0].get_bucket_policy.call_args.kwargs["ExpectedBucketOwner"]
            == request.plan.source_identity.source_aws_account_id
        )
        assert (
            storage[1].head_bucket.call_args.kwargs["ExpectedBucketOwner"]
            == request.plan.destination_identity.destination_aws_account_id
        )
    else:
        with pytest.raises((LifecycleConflict, ValueError)):
            await boundary.validate(request, run)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "changed", "retained", "already_absent"])
async def test_observed_log_cleanup_checks_integrity_without_production_completeness(
    tmp_path: Path, fault: str | None
) -> None:
    request = request_fixture()
    payload = request.model_dump(mode="json")
    for side in ("source_identity", "destination_identity"):
        payload["plan"][side]["environment"] = "test"
    payload["plan"]["runs"][0]["source"]["original_resources"].update(s3_bucket="source", log_group="logs")
    payload["plan"]["runs"][0]["source"]["log_group"] = f"logs/{request.plan.runs[0].source.run_id}"
    payload["plan"]["runs"][0]["destination"]["original_resources"].update(s3_bucket="destination", log_group="logs")
    payload["plan"]["runs"][0]["destination"]["log_group"] = f"logs/{request.plan.runs[0].source.run_id}"
    request = TransferRequest.model_validate(payload)
    deleted: list[str] = []

    class DeletableLogs(FakeLogs):
        def delete_log_group(self, *, logGroupName: str) -> None:
            deleted.append(logGroupName)
            self.absent = fault != "retained"

    logs, objects = DeletableLogs(), FakeS3()
    source, destination = Mock(), Mock()
    source.boto3_session.return_value = FakeSession("111111111111", logs)
    destination.boto3_session.return_value = FakeSession("222222222222", objects)
    boundary = ObservedEventsBoundary(source, destination, tmp_path)
    archive, _ = await boundary.archive(request, request.plan.runs[0])
    if fault == "changed":
        logs.events[0]["message"] = "late event"
    elif fault == "already_absent":
        logs.absent = True
    if fault in {"changed", "retained"}:
        with pytest.raises(LifecycleConflict):
            await boundary.cleanup_logs(request, request.plan.runs[0], archive)
    else:
        await boundary.cleanup_logs(request, request.plan.runs[0], archive)
        await boundary.cleanup_logs(request, request.plan.runs[0], archive)
    assert deleted == ([] if fault in {"changed", "already_absent"} else [request.plan.runs[0].source.log_group])
    assert objects.objects


def test_selected_profiles_and_regions_are_preserved_for_every_transfer_client(monkeypatch: pytest.MonkeyPatch) -> None:
    synchronous = Mock()
    asynchronous = Mock()
    sync_factory = Mock(return_value=synchronous)
    async_factory = Mock(return_value=asynchronous)
    monkeypatch.setattr("tracker.run_transfer.providers.boto3.Session", sync_factory)
    monkeypatch.setattr("tracker.run_transfer.providers.aioboto3.Session", async_factory)
    source = ProfileClients("transfer-source", "us-east-1")
    destination = ProfileClients("transfer-destination", "us-east-1").with_region("us-west-2")

    for clients in (source, destination):
        sync_factory.reset_mock()
        async_factory.reset_mock()
        synchronous.reset_mock()
        asynchronous.reset_mock()
        clients.sts_client()
        clients.secretsmanager_client()
        clients.cloudwatch_logs_client()
        clients.s3_client()
        clients.secretsmanager_async_client()
        assert synchronous.client.call_args_list == [call("sts"), call("secretsmanager"), call("logs")]
        assert asynchronous.client.call_args_list == [call("s3"), call("secretsmanager")]
        assert sync_factory.call_args_list == [call(profile_name=clients.profile, region_name=clients.region)] * 3
        assert async_factory.call_args_list == [call(profile_name=clients.profile, region_name=clients.region)] * 2
        clients.sts_client()
        clients.secretsmanager_client()
        clients.cloudwatch_logs_client()
        assert sync_factory.call_count == 3

    assert source.region == "us-east-1"
    assert destination.region == "us-west-2"
    assert destination.profile == "transfer-destination"
