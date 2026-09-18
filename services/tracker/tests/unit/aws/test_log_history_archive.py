"""Archive transport must retain history and fail closed across partial uploads."""

from copy import deepcopy
from importlib import import_module
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from botocore.exceptions import ClientError

from tracker.aws.runtime import AWSResources
from tracker.lifecycle import OperationIdentity, RunScope

RUN_ID = UUID("00000000-0000-0000-0000-000000000001")
OPERATION_ID = UUID("00000000-0000-0000-0000-000000000002")
SOURCE_ACCOUNT = "111111111111"
DESTINATION_ACCOUNT = "222222222222"


class FakeLogs:
    def __init__(self) -> None:
        self.meta = SimpleNamespace(region_name="us-east-1")
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.scan = 0
        self.changed = False
        self.absent = False
        self.failure: str | None = None
        self.events = [
            {
                "timestamp": 1,
                "ingestionTime": 2,
                "message": "private old message",
                "eventId": "first",
                "logStreamName": "old",
            },
            {
                "timestamp": 1,
                "ingestionTime": 2,
                "message": "private old message",
                "eventId": "second",
                "logStreamName": "old",
            },
        ]

    def describe_log_groups(self, **request: Any) -> dict[str, Any]:
        self.requests.append(("groups", request))
        self.scan += 1
        return {
            "logGroups": []
            if self.absent
            else [
                {
                    "logGroupName": f"logs/{RUN_ID}",
                    "arn": f"arn:aws:logs:us-east-1:{SOURCE_ACCOUNT}:log-group:logs/{RUN_ID}:*",
                }
            ]
        }

    def describe_log_streams(self, **request: Any) -> dict[str, Any]:
        self.requests.append(("streams", request))
        if "nextToken" not in request:
            return {"logStreams": [], "nextToken": "stream-page"}
        return {"logStreams": [{"logStreamName": "empty"}, {"logStreamName": "old"}]}

    def filter_log_events(self, **request: Any) -> dict[str, Any]:
        self.requests.append(("events", request))
        if self.failure:
            raise ClientError({"Error": {"Code": self.failure, "Message": "private provider text"}}, "FilterLogEvents")
        if "nextToken" not in request:
            return {"events": [], "nextToken": "event-page"}
        events = deepcopy(self.events)
        if self.changed and self.scan == 2:
            events[0]["message"] = "changed"
        return {"events": events}


class FakeS3:
    def __init__(self) -> None:
        self.meta = SimpleNamespace(region_name="us-west-2")
        self.objects: dict[tuple[str, str], bytes] = {}
        self.requests: list[tuple[str, dict[str, Any]]] = []
        self.uncertain = False
        self.corrupt = False
        self.versioning = "Enabled"
        self.owner_id = "42"

    def head_bucket(self, **request: Any) -> dict[str, Any]:
        self.requests.append(("bucket", request))
        return {"BucketRegion": "us-west-2"}

    def get_bucket_tagging(self, **request: Any) -> dict[str, Any]:
        return {
            "TagSet": [
                {"Key": "valsmith:owner-account-id", "Value": self.owner_id},
                {"Key": "valsmith:environment", "Value": "test"},
                {"Key": "valsmith:valkyrie-org-id", "Value": str(UUID(int=3))},
                {"Key": "valsmith:backup", "Value": "true"},
            ]
        }

    def get_bucket_versioning(self, **request: Any) -> dict[str, Any]:
        return {"Status": self.versioning}

    def get_bucket_ownership_controls(self, **request: Any) -> dict[str, Any]:
        return {"OwnershipControls": {"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]}}

    def put_object(self, **request: Any) -> dict[str, Any]:
        self.requests.append(("put", request))
        version = f"version-{len(self.objects)}"
        self.objects[(request["Key"], version)] = request["Body"]
        if self.uncertain:
            raise TimeoutError("private network detail")
        return {"VersionId": version}

    def get_object(self, **request: Any) -> dict[str, Any]:
        self.requests.append(("get", request))
        content = self.objects[(request["Key"], request["VersionId"])]
        return {
            "VersionId": request["VersionId"],
            "ServerSideEncryption": "AES256",
            "ContentLength": len(content),
            "Body": BytesIO(b"bad" if self.corrupt else content),
        }


class FakeSession:
    def __init__(self, account: str, client: Any) -> None:
        self.account = account
        self.service = client

    def client(self, service: str, **kwargs: Any) -> Any:
        return self if service == "sts" else self.service

    def get_caller_identity(self) -> dict[str, str]:
        return {"Account": self.account, "Arn": f"arn:aws:iam::{self.account}:role/archive"}


@pytest.fixture
def archive() -> Any:
    try:
        return import_module("tracker.aws.log_history_archive")
    except ModuleNotFoundError:

        class MissingArchive:
            def __getattr__(self, name: str) -> Any:
                pytest.fail("historical archive provider is not implemented")

        return MissingArchive()


def scoped_input(archive: Any, **updates: Any) -> Any:
    identity = OperationIdentity(
        operation_id=OPERATION_ID,
        parent_plan_sha256="a" * 64,
        github_owner_id=42,
        org_id=UUID(int=3),
        source_aws_account_id=SOURCE_ACCOUNT,
        destination_aws_account_id=DESTINATION_ACCOUNT,
        region="us-east-1",
        environment="test",
        database_target="source",
        run_ids=(RUN_ID,),
    )
    source = RunScope(
        run_id=RUN_ID,
        original_resources=AWSResources(
            region="us-east-1", s3_bucket="source", log_group="logs", log_retention_days=30
        ),
    )
    destination = RunScope(
        run_id=RUN_ID,
        original_resources=AWSResources(
            region="us-west-2", s3_bucket="destination", log_group="logs", log_retention_days=30
        ),
    )
    return archive.FrozenLogScope(
        source_identity=identity,
        destination_identity=identity.model_copy(update={"region": "us-west-2", "database_target": "destination"}),
        source=source,
        destination=destination,
        freeze_evidence_sha256="b" * 64,
        unmasked_read_authorized=True,
        **updates,
    )


def run_archive(archive: Any, tmp_path: Path, logs: FakeLogs, storage: FakeS3, **updates: Any) -> Any:
    return archive.archive_logs(
        scoped_input(archive),
        source_session=FakeSession(SOURCE_ACCOUNT, logs),
        destination_session=FakeSession(DESTINATION_ACCOUNT, storage),
        journal_directory=tmp_path,
        **updates,
    )


def test_archive_preserves_old_order_multiplicity_and_empty_streams(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    result = run_archive(archive, tmp_path, logs, storage)
    manifest = archive.read_manifest(result.reference, scoped_input(archive), FakeSession(DESTINATION_ACCOUNT, storage))
    events = list(archive.read_events(manifest, scoped_input(archive), FakeSession(DESTINATION_ACCOUNT, storage)))

    assert [
        (event.timestamp, event.message, event.event_id, event.ingestion_time, event.stream_name, event.ordinal)
        for event in events
    ] == [(1, "private old message", "first", 2, "old", 0), (1, "private old message", "second", 2, "old", 1)]
    assert manifest.stream_names == ("empty", "old")
    assert manifest.first_scan == manifest.second_scan
    assert result.event_count == 2
    assert "private old message" not in result.model_dump_json()
    assert logs.scan == 2
    assert all(
        request.get("unmask") is True and "filterPattern" not in request
        for method, request in logs.requests
        if method == "events"
    )
    assert all(request["ExpectedBucketOwner"] == DESTINATION_ACCOUNT for _, request in storage.requests)
    assert all(
        request["ServerSideEncryption"] == "AES256" and request["IfNoneMatch"] == "*"
        for method, request in storage.requests
        if method == "put"
    )


def test_changed_second_scan_never_publishes_manifest(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    logs.changed = True
    with pytest.raises(archive.ArchiveError, match="source changed"):
        run_archive(archive, tmp_path, logs, storage)
    assert not any(key.endswith("manifest.json") for key, _ in storage.objects)


def test_unknown_upload_stays_unresolved_on_resume(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    storage.uncertain = True
    with pytest.raises(archive.ArchiveError):
        run_archive(archive, tmp_path, logs, storage)
    storage.uncertain = False
    with pytest.raises(archive.ArchiveError, match="unresolved upload"):
        run_archive(archive, tmp_path, logs, storage)
    assert len(storage.objects) == 1
    assert all("private old message" not in path.read_text() for path in tmp_path.rglob("*.json"))


def test_known_versions_resume_without_upload_and_detect_corruption(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    first = run_archive(archive, tmp_path, logs, storage)
    second = run_archive(archive, tmp_path, logs, storage)
    assert first == second
    assert len(storage.objects) == 2
    storage.corrupt = True
    with pytest.raises(archive.ArchiveError, match="content verification"):
        run_archive(archive, tmp_path, logs, storage)


@pytest.mark.parametrize("failure", ["AccessDeniedException", "ResourceNotFoundException"])
def test_source_errors_are_not_empty_history(archive: Any, tmp_path: Path, failure: str) -> None:
    logs, storage = FakeLogs(), FakeS3()
    logs.failure = failure
    with pytest.raises(archive.ArchiveError) as error:
        run_archive(archive, tmp_path, logs, storage)
    assert "private provider text" not in str(error.value)
    assert storage.objects == {}


def test_absent_group_requires_two_exact_inventories(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    logs.absent = True
    result = run_archive(archive, tmp_path, logs, storage)
    manifest = archive.read_manifest(result.reference, scoped_input(archive), FakeSession(DESTINATION_ACCOUNT, storage))
    assert manifest.first_scan.group_absent is True
    assert manifest.second_scan.group_absent is True
    assert manifest.stream_names == ()
    assert result.event_count == 0
    assert len(storage.objects) == 1


@pytest.mark.parametrize("problem", ["account", "region", "versioning"])
def test_wrong_authority_or_bucket_fails_before_writes(archive: Any, tmp_path: Path, problem: str) -> None:
    logs, storage = FakeLogs(), FakeS3()
    source = FakeSession("999999999999" if problem == "account" else SOURCE_ACCOUNT, logs)
    if problem == "region":
        storage.meta.region_name = "us-east-1"
    if problem == "versioning":
        storage.versioning = "Suspended"
    with pytest.raises(archive.ArchiveError):
        archive.archive_logs(
            scoped_input(archive),
            source_session=source,
            destination_session=FakeSession(DESTINATION_ACCOUNT, storage),
            journal_directory=tmp_path,
        )
    assert storage.objects == {}


def test_limits_reject_large_events_without_unbounded_upload(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    logs.events[0]["message"] = "x" * 1024
    with pytest.raises(archive.ArchiveError, match="limit"):
        run_archive(archive, tmp_path, logs, storage, limits=archive.ArchiveLimits(chunk_bytes=512))
    assert storage.objects == {}


def test_bucket_owner_tag_mismatch_fails_before_writes(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    storage.owner_id = "99"
    with pytest.raises(archive.ArchiveError, match="owner scope"):
        run_archive(archive, tmp_path, logs, storage)
    assert storage.objects == {}


def test_same_session_cannot_supply_both_authorities(archive: Any, tmp_path: Path) -> None:
    logs = FakeLogs()
    session = FakeSession(SOURCE_ACCOUNT, logs)
    with pytest.raises(archive.ArchiveError, match="separate"):
        archive.archive_logs(
            scoped_input(archive), source_session=session, destination_session=session, journal_directory=tmp_path
        )


def test_recorded_version_survives_failed_readback(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    storage.corrupt = True
    with pytest.raises(archive.ArchiveError, match="content verification"):
        run_archive(archive, tmp_path, logs, storage)
    storage.corrupt = False
    result = run_archive(archive, tmp_path, logs, storage)
    assert result.event_count == 2
    assert len(storage.objects) == 2


def test_bounded_chunks_keep_identical_event_occurrences(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    logs.events *= 20
    result = run_archive(archive, tmp_path, logs, storage, limits=archive.ArchiveLimits(chunk_bytes=512))
    manifest = archive.read_manifest(result.reference, scoped_input(archive), FakeSession(DESTINATION_ACCOUNT, storage))
    events = list(archive.read_events(manifest, scoped_input(archive), FakeSession(DESTINATION_ACCOUNT, storage)))
    assert len(events) == 40
    assert [event.ordinal for event in events] == list(range(40))
    assert result.chunk_count > 1
    assert all(len(content) <= 512 for (key, _), content in storage.objects.items() if "/chunks/" in key)


def test_empty_token_cycle_is_not_success(archive: Any, tmp_path: Path) -> None:
    class CyclicLogs(FakeLogs):
        def filter_log_events(self, **request: Any) -> dict[str, Any]:
            return {"events": [], "nextToken": "never-finished"}

    storage = FakeS3()
    with pytest.raises(archive.ArchiveError, match="pagination"):
        run_archive(archive, tmp_path, CyclicLogs(), storage)
    assert storage.objects == {}


def test_version_store_refuses_foreign_run_writes(archive: Any) -> None:
    storage = FakeS3()
    store = archive.ArchiveVersionStore(FakeSession(DESTINATION_ACCOUNT, storage), scoped_input(archive).location)
    with pytest.raises(archive.ArchiveError, match="scope"):
        store.write("benchmarks/foreign/log-history/manifest.json", b"{}")
    assert storage.objects == {}


def test_manifest_limit_failure_keeps_only_private_chunks(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    with pytest.raises(archive.ArchiveError, match="byte limit"):
        run_archive(archive, tmp_path, logs, storage, limits=archive.ArchiveLimits(manifest_bytes=1024))
    assert len(storage.objects) == 1
    assert not any(key.endswith("manifest.json") for key, _ in storage.objects)


def test_changed_parent_plan_cannot_reuse_journal(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    run_archive(archive, tmp_path, logs, storage)
    scope = scoped_input(archive)
    changed_scope = scope.model_copy(
        update={
            "source_identity": scope.source_identity.model_copy(update={"parent_plan_sha256": "c" * 64}),
            "destination_identity": scope.destination_identity.model_copy(update={"parent_plan_sha256": "c" * 64}),
        }
    )
    with pytest.raises(archive.ArchiveError, match="journal identity"):
        archive.archive_logs(
            changed_scope,
            source_session=FakeSession(SOURCE_ACCOUNT, logs),
            destination_session=FakeSession(DESTINATION_ACCOUNT, storage),
            journal_directory=tmp_path,
        )
    assert len(storage.objects) == 2


def test_changed_empty_stream_inventory_blocks_manifest(archive: Any, tmp_path: Path) -> None:
    class ChangingStreams(FakeLogs):
        def describe_log_streams(self, **request: Any) -> dict[str, Any]:
            response = super().describe_log_streams(**request)
            if self.scan == 2 and response.get("logStreams"):
                response["logStreams"].append({"logStreamName": "new-empty-stream"})
            return response

    storage = FakeS3()
    with pytest.raises(archive.ArchiveError, match="source changed"):
        run_archive(archive, tmp_path, ChangingStreams(), storage)
    assert not any(key.endswith("manifest.json") for key, _ in storage.objects)


def test_exact_group_scan_follows_empty_pages(archive: Any, tmp_path: Path) -> None:
    class PagedGroups(FakeLogs):
        def describe_log_groups(self, **request: Any) -> dict[str, Any]:
            if "nextToken" not in request:
                return {"logGroups": [], "nextToken": "group-next"}
            return super().describe_log_groups(**request)

    logs, storage = PagedGroups(), FakeS3()
    result = run_archive(archive, tmp_path, logs, storage)
    manifest = archive.read_manifest(result.reference, scoped_input(archive), FakeSession(DESTINATION_ACCOUNT, storage))
    assert manifest.first_scan.group_pages == 2
    assert manifest.second_scan.group_pages == 2
    assert result.event_count == 2


def test_null_returned_version_leaves_unresolved_intent(archive: Any, tmp_path: Path) -> None:
    class NullVersionS3(FakeS3):
        def put_object(self, **request: Any) -> dict[str, Any]:
            super().put_object(**request)
            return {"VersionId": "null"}

    logs, storage = FakeLogs(), NullVersionS3()
    with pytest.raises(archive.ArchiveError, match="immutable version"):
        run_archive(archive, tmp_path, logs, storage)
    with pytest.raises(archive.ArchiveError, match="unresolved upload"):
        run_archive(archive, tmp_path, logs, storage)
    assert len(storage.objects) == 1


def test_missing_manifest_never_falls_back_to_empty(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    result = run_archive(archive, tmp_path, logs, storage)
    storage.objects.pop((result.reference.manifest.key, result.reference.manifest.version_id))
    with pytest.raises(archive.ArchiveError, match="manifest verification"):
        archive.read_manifest(
            result.reference, scoped_input(archive).location, FakeSession(DESTINATION_ACCOUNT, storage)
        )


def test_changed_destination_scope_cannot_read_manifest(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    result = run_archive(archive, tmp_path, logs, storage)
    location = scoped_input(archive).location.model_copy(update={"bucket": "different-bucket"})
    with pytest.raises(archive.ArchiveError, match="manifest identity"):
        archive.read_manifest(result.reference, location, FakeSession(DESTINATION_ACCOUNT, storage))


def test_chunk_wrong_version_response_is_rejected(archive: Any, tmp_path: Path) -> None:
    class WrongVersionS3(FakeS3):
        def get_object(self, **request: Any) -> dict[str, Any]:
            response = super().get_object(**request)
            response["VersionId"] = "foreign-version"
            return response

    with pytest.raises(archive.ArchiveError, match="content verification"):
        run_archive(archive, tmp_path, FakeLogs(), WrongVersionS3())


def test_existing_group_with_no_streams_is_not_absent(archive: Any, tmp_path: Path) -> None:
    class EmptyLogs(FakeLogs):
        def describe_log_streams(self, **request: Any) -> dict[str, Any]:
            return {"logStreams": []}

    logs, storage = EmptyLogs(), FakeS3()
    logs.events = []
    result = run_archive(archive, tmp_path, logs, storage)
    manifest = archive.read_manifest(
        result.reference, scoped_input(archive).location, FakeSession(DESTINATION_ACCOUNT, storage)
    )
    assert manifest.first_scan.group_absent is False
    assert result.event_count == 0
    assert result.stream_count == 0
    assert (
        list(archive.read_events(manifest, scoped_input(archive).location, FakeSession(DESTINATION_ACCOUNT, storage)))
        == []
    )


def test_empty_archive_iterator_still_checks_destination_scope(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    logs.absent = True
    result = run_archive(archive, tmp_path, logs, storage)
    location = scoped_input(archive).location
    session = FakeSession(DESTINATION_ACCOUNT, storage)
    manifest = archive.read_manifest(result.reference, location, session)
    changed_location = location.model_copy(update={"bucket": "foreign-bucket"})
    with pytest.raises(archive.ArchiveError, match="destination"):
        list(archive.read_events(manifest, changed_location, session))


@pytest.mark.parametrize("remaining_events", [0, 2])
def test_unknown_prior_chunk_blocks_smaller_scan(archive: Any, tmp_path: Path, remaining_events: int) -> None:
    class UncertainSecondChunk(FakeS3):
        def put_object(self, **request: Any) -> dict[str, Any]:
            self.uncertain = request["Key"].endswith("00000001.json")
            return super().put_object(**request)

    logs, storage = FakeLogs(), UncertainSecondChunk()
    logs.events *= 4
    limits = archive.ArchiveLimits(chunk_bytes=512)
    with pytest.raises(archive.ArchiveError):
        run_archive(archive, tmp_path, logs, storage, limits=limits)

    prior_versions = dict(storage.objects)
    logs.events = logs.events[:remaining_events]
    with pytest.raises(archive.ArchiveError, match="unresolved upload"):
        run_archive(archive, tmp_path, logs, storage, limits=limits)

    assert storage.objects == prior_versions
    assert not any(key.endswith("manifest.json") for key, _ in storage.objects)


def test_known_prior_chunk_cannot_disappear_on_resume(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    storage.corrupt = True
    with pytest.raises(archive.ArchiveError, match="content verification"):
        run_archive(archive, tmp_path, logs, storage)

    prior_versions = dict(storage.objects)
    storage.corrupt = False
    logs.events = []
    with pytest.raises(archive.ArchiveError, match="inventory|unexplained"):
        run_archive(archive, tmp_path, logs, storage)

    assert storage.objects == prior_versions
    assert not any(key.endswith("manifest.json") for key, _ in storage.objects)


def test_journal_is_bound_before_source_scan_to_one_operation(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    logs.failure = "AccessDeniedException"
    with pytest.raises(archive.ArchiveError):
        run_archive(archive, tmp_path, logs, storage)

    scope = scoped_input(archive)
    changed_scope = scope.model_copy(
        update={
            "source_identity": scope.source_identity.model_copy(update={"operation_id": UUID(int=999)}),
            "destination_identity": scope.destination_identity.model_copy(update={"operation_id": UUID(int=999)}),
        }
    )
    logs.failure = None
    prior_scans = logs.scan
    with pytest.raises(archive.ArchiveError, match="journal identity"):
        archive.archive_logs(
            changed_scope,
            source_session=FakeSession(SOURCE_ACCOUNT, logs),
            destination_session=FakeSession(DESTINATION_ACCOUNT, storage),
            journal_directory=tmp_path,
        )

    assert logs.scan == prior_scans
    assert storage.objects == {}


@pytest.mark.parametrize("failed_readback", [False, True])
@pytest.mark.parametrize("change", ["session", "pages", "both"])
def test_manifest_resume_preserves_original_observations(
    archive: Any, tmp_path: Path, failed_readback: bool, change: str
) -> None:
    class SessionIdentity(FakeSession):
        def __init__(self, client: FakeLogs, session_name: str) -> None:
            super().__init__(SOURCE_ACCOUNT, client)
            self.session_name = session_name

        def get_caller_identity(self) -> dict[str, str]:
            return {
                "Account": self.account,
                "Arn": f"arn:aws:sts::{self.account}:assumed-role/archive/{self.session_name}",
            }

    class PageLayout(FakeLogs):
        single_page = False

        def filter_log_events(self, **request: Any) -> dict[str, Any]:
            if self.single_page:
                self.requests.append(("events", request))
                return {"events": deepcopy(self.events)}
            return super().filter_log_events(**request)

    class ManifestReadback(FakeS3):
        fail_manifest_read = False

        def get_object(self, **request: Any) -> dict[str, Any]:
            if self.fail_manifest_read and request["Key"].endswith("manifest.json"):
                raise TimeoutError("private readback error")
            return super().get_object(**request)

    logs, storage = PageLayout(), ManifestReadback()
    source = SessionIdentity(logs, "session-one")
    destination = FakeSession(DESTINATION_ACCOUNT, storage)
    storage.fail_manifest_read = failed_readback
    original_result = None
    if failed_readback:
        with pytest.raises(archive.ArchiveError):
            archive.archive_logs(
                scoped_input(archive),
                source_session=source,
                destination_session=destination,
                journal_directory=tmp_path,
            )
    else:
        original_result = archive.archive_logs(
            scoped_input(archive), source_session=source, destination_session=destination, journal_directory=tmp_path
        )

    original_objects = dict(storage.objects)
    assert len(original_objects) == 2
    storage.fail_manifest_read = False
    logs.single_page = change in {"pages", "both"}
    source.session_name = "session-two" if change in {"session", "both"} else "session-one"
    result = archive.archive_logs(
        scoped_input(archive), source_session=source, destination_session=destination, journal_directory=tmp_path
    )
    manifest = archive.read_manifest(result.reference, scoped_input(archive).location, destination)

    assert storage.objects == original_objects
    assert result.event_count == 2
    assert manifest.source_principal_arn.endswith("/session-one")
    assert manifest.first_scan.event_pages == manifest.second_scan.event_pages == 2
    assert logs.scan == 4
    assert list(archive.read_events(manifest, scoped_input(archive).location, destination))
    if original_result is not None:
        assert result == original_result


def test_partial_scan_cannot_hide_known_chunks_without_complete_inventory(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    logs.events *= 4
    storage.corrupt = True
    limits = archive.ArchiveLimits(chunk_bytes=512)
    with pytest.raises(archive.ArchiveError, match="content verification"):
        run_archive(archive, tmp_path, logs, storage, limits=limits)

    assert not (tmp_path / "inventory.json").exists()
    original_objects = dict(storage.objects)
    storage.corrupt = False
    logs.events = []
    with pytest.raises(archive.ArchiveError, match="unexplained prior journal chunks"):
        run_archive(archive, tmp_path, logs, storage, limits=limits)

    assert storage.objects == original_objects


def test_unknown_manifest_acceptance_blocks_changed_retry(archive: Any, tmp_path: Path) -> None:
    class UncertainManifest(FakeS3):
        def put_object(self, **request: Any) -> dict[str, Any]:
            self.uncertain = request["Key"].endswith("manifest.json")
            return super().put_object(**request)

    logs, storage = FakeLogs(), UncertainManifest()
    with pytest.raises(archive.ArchiveError):
        run_archive(archive, tmp_path, logs, storage)

    original_objects = dict(storage.objects)
    assert len(original_objects) == 2
    logs.events = []
    with pytest.raises(archive.ArchiveError, match="unresolved upload"):
        run_archive(archive, tmp_path, logs, storage)

    assert storage.objects == original_objects


def test_saved_manifest_content_is_reverified_on_resume(archive: Any, tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    result = run_archive(archive, tmp_path, logs, storage)
    identity = (result.reference.manifest.key, result.reference.manifest.version_id)
    original_content = storage.objects[identity]
    storage.objects[identity] = b"x" * len(original_content)
    with pytest.raises(archive.ArchiveError, match="content verification"):
        run_archive(archive, tmp_path, logs, storage)

    assert len(storage.objects) == 2
    assert logs.scan == 4


def test_completed_inventory_retains_empty_streams_before_manifest_publication(archive: Any, tmp_path: Path) -> None:
    class FewerStreams(FakeLogs):
        def describe_log_streams(self, **request: Any) -> dict[str, Any]:
            return {"logStreams": [{"logStreamName": "old"}]}

    logs, storage = FakeLogs(), FakeS3()
    limits = archive.ArchiveLimits(manifest_bytes=1024)
    with pytest.raises(archive.ArchiveError, match="byte limit"):
        run_archive(archive, tmp_path, logs, storage, limits=limits)

    original_objects = dict(storage.objects)
    with pytest.raises(archive.ArchiveError, match="inventory conflicts"):
        run_archive(archive, tmp_path, FewerStreams(), storage, limits=limits)

    assert storage.objects == original_objects
    assert not any(key.endswith("manifest.json") for key, _ in storage.objects)


def test_existing_unbound_journal_is_not_adopted(archive: Any, tmp_path: Path) -> None:
    (tmp_path / "legacy.json").write_text("{}")
    logs, storage = FakeLogs(), FakeS3()
    with pytest.raises(archive.ArchiveError, match="unbound prior journal"):
        run_archive(archive, tmp_path, logs, storage)

    assert storage.objects == {}
    assert logs.scan == 0
