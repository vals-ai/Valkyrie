"""The paired boundary uses the real historical writer and reader."""

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from tests.relocation_support import VersionStore
from tests.unit.aws.test_historical_log_provider import LiveLogs
from tests.unit.aws.test_log_history_archive import FakeLogs, FakeS3, FakeSession, scoped_input
from tests.unit.test_relocation_providers import setup
from tracker.aws import log_history_archive
from tracker.aws.historical_logs import HistoricalLogProvider
from tracker.lifecycle import LifecycleConflict
from tracker.run_transfer.contracts import TransferRequest
from tracker.run_transfer.providers import TransferAWSBoundary
from tracker.run_transfer.references import verify_portable_references
from tracker.run_transfer.rows import RowClosure
from tracker.runtime.logs import LogPage, RunLogReference


def test_paired_archive_uses_verified_exact_versions_and_actual_reader(tmp_path: Path):

    scope = scoped_input(log_history_archive)
    request = TransferRequest.model_validate(
        {
            "nonce": str(uuid4()),
            "action": "import",
            "plan": {
                "source_identity": scope.source_identity.model_dump(mode="json"),
                "destination_identity": scope.destination_identity.model_dump(mode="json"),
                "org_name": "test",
                "runs": [
                    {
                        "source": scope.source.model_dump(mode="json"),
                        "destination": scope.destination.model_dump(mode="json"),
                        "source_rows_sha256": "a" * 64,
                        "execution_policy": "history_only",
                        "unmasked_read_authorized": True,
                    }
                ],
            },
        }
    )
    logs, storage = FakeLogs(), FakeS3()
    boundary = TransferAWSBoundary(
        None,
        None,
        tmp_path,
        source_session=FakeSession("111111111111", logs),
        destination_session=FakeSession("222222222222", storage),
    )
    run = request.plan.runs[0]
    archive = asyncio.run(boundary.archive(request, run))
    asyncio.run(boundary.verify_archive(request, run, archive))
    assert archive.event_count == 2
    assert "private old message" not in archive.model_dump_json()
    storage.corrupt = True

    with pytest.raises(Exception):
        asyncio.run(boundary.verify_archive(request, run, archive))


def test_portable_reference_verification_uses_metadata_and_rejects_unknown_location(tmp_path: Path) -> None:

    class Metadata:
        def describe_secret(self, **arguments: str) -> dict[str, object]:
            assert arguments == {"SecretId": "provider-reference"}
            return {
                "ARN": "arn:aws:secretsmanager:us-west-2:222222222222:secret:provider-reference-ABC",
                "VersionIdsToStages": {"v1": ["AWSCURRENT"]},
            }

    class Session:
        def client(self, service: str, **options: str) -> Metadata:
            assert service == "secretsmanager"
            assert options == {"region_name": "us-west-2"}
            return Metadata()

    closure = RowClosure(
        {
            "benchmark": [
                {
                    "arguments": {
                        "properties": {},
                        "dataset": None,
                        "sandbox_provider_secret_name": "provider-reference",
                        "contract": {},
                    },
                    "webhook_secret_name": None,
                    "custom_benchmark_service": None,
                }
            ]
        },
        {},
    )
    proof = verify_portable_references(closure, Session(), "222222222222", "us-west-2")
    assert len(proof) == 64
    closure.rows["benchmark"][0]["arguments"]["dataset"] = "https://unknown.example/data"
    with pytest.raises(LifecycleConflict):
        verify_portable_references(closure, Session(), "222222222222", "us-west-2")


def test_paired_version_verifier_uses_separate_accounts_and_accepts_legacy_null_source(tmp_path: Path) -> None:

    _, original, old = setup()
    identity = {**old["plan"]["identity"], "destination_aws_account_id": "222222222222"}
    run = old["plan"]["runs"][0]
    old["copied_objects"][0]["source_version_id"] = "null"

    class AccountStore(VersionStore):
        def __init__(self, account: str, bucket: str) -> None:
            super().__init__(str(run["scope"]["run_id"]))
            self.account, self.bucket = account, bucket

        def bound(self, arguments: dict[str, Any]) -> dict[str, Any]:
            assert arguments["ExpectedBucketOwner"] == self.account
            assert arguments["Bucket"] == self.bucket
            return {**arguments, "ExpectedBucketOwner": "123456789012"}

        async def get_bucket_policy(self, **arguments: Any) -> dict[str, str]:
            return await super().get_bucket_policy(**self.bound(arguments))

        async def list_object_versions(self, **arguments: Any) -> dict[str, Any]:
            return await super().list_object_versions(**self.bound(arguments))

        async def get_object(self, **arguments: Any) -> dict[str, Any]:
            return await super().get_object(**self.bound(arguments))

        async def list_multipart_uploads(self, **arguments: Any) -> dict[str, Any]:
            return await super().list_multipart_uploads(**self.bound(arguments))

    source, destination = AccountStore("123456789012", "source"), AccountStore("222222222222", "destination")
    source.fence_statement = original.fence_statement
    source.versioning["source"] = {}
    source.versions["source"] = [("null", b'{"value":1}')]
    source_clients, destination_clients = Mock(), Mock()
    source_clients.s3_client.return_value = source
    destination_clients.s3_client.return_value = destination
    request = TransferRequest.model_validate(
        {
            "nonce": str(uuid4()),
            "action": "import",
            "plan": {
                "source_identity": identity,
                "destination_identity": {
                    **identity,
                    "database_target": "postgresql:destination:5432/tracker",
                    "region": "us-west-2",
                },
                "org_name": "test",
                "runs": [
                    {
                        "source": run["scope"],
                        "destination": {
                            "run_id": run["scope"]["run_id"],
                            "original_resources": {**run["destination_resources"], "region": "us-west-2"},
                        },
                        "source_rows_sha256": "a" * 64,
                        "execution_policy": "history_only",
                        "unmasked_read_authorized": True,
                    }
                ],
            },
            "copied_objects": old["copied_objects"],
            "destination_versions": old["destination_versions"],
        }
    )
    boundary = TransferAWSBoundary(source_clients, destination_clients, tmp_path)
    asyncio.run(boundary.verify_objects(request, request.plan.runs[0]))
    destination.versions["destination"] = [("d1", b"wrong")]
    with pytest.raises(LifecycleConflict):
        asyncio.run(boundary.verify_objects(request, request.plan.runs[0]))


def archive_boundary(tmp_path: Path) -> tuple[TransferRequest, TransferAWSBoundary, FakeLogs, FakeS3]:
    scope = scoped_input(log_history_archive)
    request = TransferRequest.model_validate(
        {
            "nonce": str(uuid4()),
            "action": "import",
            "plan": {
                "source_identity": scope.source_identity.model_dump(mode="json"),
                "destination_identity": scope.destination_identity.model_dump(mode="json"),
                "org_name": "test",
                "runs": [
                    {
                        "source": scope.source.model_dump(mode="json"),
                        "destination": scope.destination.model_dump(mode="json"),
                        "source_rows_sha256": "a" * 64,
                        "execution_policy": "history_only",
                        "unmasked_read_authorized": True,
                    }
                ],
            },
        }
    )
    logs, storage = FakeLogs(), FakeS3()
    boundary = TransferAWSBoundary(
        Mock(),
        Mock(),
        tmp_path,
        source_session=FakeSession("111111111111", logs),
        destination_session=FakeSession("222222222222", storage),
    )
    return request, boundary, logs, storage


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["event_count", "stream_count", "chunk_count", "event_sha256"])
async def test_archive_receipt_must_match_verified_manifest(tmp_path: Path, field: str) -> None:
    request, boundary, _, storage = archive_boundary(tmp_path)
    run = request.plan.runs[0]
    receipt = await boundary.archive(request, run)
    receipt = receipt.model_copy(update={field: "f" * 64 if field == "event_sha256" else 99})
    objects = dict(storage.objects)

    with pytest.raises(LifecycleConflict, match="receipt differs"):
        await boundary.verify_archive(request, run, receipt)

    assert storage.objects == objects


@pytest.mark.asyncio
async def test_transfer_rechecks_full_event_proof(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    request, boundary, _, storage = archive_boundary(tmp_path)
    run = request.plan.runs[0]
    receipt = await boundary.archive(request, run)
    monkeypatch.setattr("tracker.run_transfer.providers.read_events", Mock(return_value=iter(())))

    with pytest.raises(LifecycleConflict, match="full event proof"):
        await boundary.verify_archive(request, run, receipt)

    assert len(storage.objects) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["cycle", "count", "pages"])
async def test_transfer_exhausts_actual_reader_and_requires_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    request, boundary, _, _ = archive_boundary(tmp_path)
    run = request.plan.runs[0]
    receipt = await boundary.archive(request, run)
    provider = Mock()
    if fault == "cycle":
        provider.fetch = AsyncMock(side_effect=[LogPage([], "repeat"), LogPage([], "repeat")])
    elif fault == "count":
        provider.fetch = AsyncMock(return_value=LogPage([]))
    else:
        live = LiveLogs()
        live.events = []
        actual = HistoricalLogProvider(
            receipt.reference,
            scoped_input(log_history_archive).location,
            boundary.destination_session,
            live,
            terminal=True,
        )
        first = await actual.fetch(RunLogReference(run.source.run_id), limit=1)
        second = await actual.fetch(RunLogReference(run.source.run_id), cursor=first.next_cursor)
        provider.fetch = AsyncMock(side_effect=[first, second])
    monkeypatch.setattr("tracker.run_transfer.providers.HistoricalLogProvider", Mock(return_value=provider))

    if fault == "pages":
        await boundary.verify_archive(request, run, receipt)
        assert provider.fetch.await_count == 2
        assert provider.fetch.await_args_list[1].kwargs["cursor"] is not None
    else:
        with pytest.raises(LifecycleConflict, match="pagination repeated|reader count differs"):
            await boundary.verify_archive(request, run, receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup", [False, True])
async def test_provider_drain_uses_saved_locator_and_two_absence_checks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup: bool
) -> None:
    request, boundary, _, _ = archive_boundary(tmp_path)
    provider = Mock()
    provider.cleanup_sandboxes = AsyncMock()
    provider.verify_absence = AsyncMock()
    factory = Mock(return_value=provider)
    monkeypatch.setattr("tracker.run_transfer.providers.RelocationAWSBoundary", factory)
    await boundary.drain(
        request,
        request.plan.runs[0],
        {"sandbox_provider": "daytona", "sandbox_provider_secret_name": "exact-source-secret"},
        cleanup=cleanup,
    )

    assert [call[0] for call in provider.mock_calls] == (["cleanup_sandboxes"] if cleanup else []) + [
        "verify_absence",
        "verify_absence",
    ]
    saved = provider.verify_absence.await_args.args[0]
    assert saved.provider.secret_name == "exact-source-secret"
    assert saved.scope == request.plan.runs[0].source
    factory.assert_called_once_with(boundary.source)


@pytest.mark.asyncio
async def test_missing_provider_locator_fails_before_provider_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request, boundary, _, _ = archive_boundary(tmp_path)
    factory = Mock()
    monkeypatch.setattr("tracker.run_transfer.providers.RelocationAWSBoundary", factory)

    with pytest.raises(LifecycleConflict, match="saved source provider secret"):
        await boundary.drain(request, request.plan.runs[0], {"sandbox_provider": "daytona"}, cleanup=True)

    factory.assert_not_called()
