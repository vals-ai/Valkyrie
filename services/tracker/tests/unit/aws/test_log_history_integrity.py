"""Invalid provider data and damaged journals must never publish trusted history."""

import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from tests.unit.aws.test_log_history_archive import (
    DESTINATION_ACCOUNT,
    RUN_ID,
    FakeLogs,
    FakeS3,
    FakeSession,
    run_archive,
    scoped_input,
)
from tracker.aws import log_history_archive as archive
from tracker.aws.log_history_store import UploadJournal, digest, encode
from tracker.runtime.log_history import ArchiveObject


@pytest.mark.parametrize(
    ("method", "response", "message"),
    [
        ("describe_log_groups", {"logGroups": [{"ignored": True}] * 51}, "group page limit"),
        (
            "describe_log_groups",
            {"logGroups": [{"logGroupName": f"logs/{RUN_ID}", "arn": "arn:aws:logs:us-east-1:999999999999:wrong"}]},
            "group identity",
        ),
        ("describe_log_streams", {"logStreams": [{"ignored": True}] * 51}, "stream page limit"),
        ("describe_log_streams", {"logStreams": [{"logStreamName": "old"}] * 2}, "stream inventory"),
        ("describe_log_streams", {"logStreams": [{"logStreamName": ""}]}, "stream inventory"),
        ("filter_log_events", {"events": [{"ignored": True}] * 10001}, "event page limit"),
        ("filter_log_events", {"events": [{"message": "private invalid event"}]}, "unsupported source event"),
        (
            "filter_log_events",
            {
                "events": [
                    {
                        "timestamp": 1,
                        "ingestionTime": 2,
                        "message": "private",
                        "eventId": "id",
                        "logStreamName": "missing",
                    }
                ]
            },
            "stream missing",
        ),
    ],
)
def test_invalid_source_inventory_never_publishes_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, method: str, response: dict[str, Any], message: str
) -> None:
    logs, storage = FakeLogs(), FakeS3()
    monkeypatch.setattr(logs, method, Mock(return_value=response))

    with pytest.raises(archive.ArchiveError, match=message):
        run_archive(archive, tmp_path, logs, storage)

    assert storage.objects == {}
    assert not any(method == "put" for method, _ in storage.requests)


@pytest.mark.parametrize("limit", ["max_pages", "max_streams", "max_chunks"])
def test_inventory_limits_stop_before_manifest_publication(tmp_path: Path, limit: str) -> None:
    logs, storage = FakeLogs(), FakeS3()
    logs.events *= 4
    limits = archive.ArchiveLimits.model_validate({limit: 1, "chunk_bytes": 512})

    with pytest.raises(archive.ArchiveError, match="limit exceeded"):
        run_archive(archive, tmp_path, logs, storage, limits=limits)

    assert not any(key.endswith("manifest.json") for key, _ in storage.objects)
    assert len(storage.objects) <= 1


def test_chunk_envelope_counts_toward_byte_limit(tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    logs.events[0]["message"] = "x" * 300

    with pytest.raises(archive.ArchiveError, match="archive chunk byte limit"):
        run_archive(archive, tmp_path, logs, storage, limits=archive.ArchiveLimits(chunk_bytes=512))

    assert storage.objects == {}


def test_group_prefix_neighbor_is_not_adopted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    logs, storage = FakeLogs(), FakeS3()
    monkeypatch.setattr(
        logs, "describe_log_groups", Mock(return_value={"logGroups": [{"logGroupName": f"logs/{RUN_ID}-other"}]})
    )
    result = run_archive(archive, tmp_path, logs, storage)
    manifest = archive.read_manifest(result.reference, scoped_input(archive), FakeSession(DESTINATION_ACCOUNT, storage))

    assert manifest.first_scan.group_absent
    assert result.event_count == 0
    assert len(storage.objects) == 1
    assert not any(method in {"events", "streams"} for method, _ in logs.requests)


@pytest.mark.parametrize("problem", ["bucket_region", "ownership"])
def test_bucket_authority_failure_precedes_source_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    logs, storage = FakeLogs(), FakeS3()
    if problem == "bucket_region":
        monkeypatch.setattr(storage, "head_bucket", Mock(return_value={"BucketRegion": "us-east-1"}))
    else:
        monkeypatch.setattr(
            storage, "get_bucket_ownership_controls", Mock(return_value={"OwnershipControls": {"Rules": []}})
        )

    with pytest.raises(archive.ArchiveError, match="bucket"):
        run_archive(archive, tmp_path, logs, storage)

    assert logs.scan == 0
    assert storage.objects == {}


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        ("oversized", "journal size limit"),
        ("array", "invalid journal record"),
        ("inventory", "inventory identity"),
        ("unknown_file", "unexplained prior journal"),
        ("object_scope", "object scope"),
        ("object_identity", "journal identity"),
        ("filename", "object identity"),
    ],
)
def test_damaged_journal_is_rejected_before_rescan_or_upload(tmp_path: Path, fault: str, message: str) -> None:
    logs, storage = FakeLogs(), FakeS3()
    run_archive(archive, tmp_path, logs, storage)
    original_objects = dict(storage.objects)
    prior_scans = logs.scan
    if fault in {"oversized", "array"}:
        (tmp_path / "scope.json").write_text(" " * 16385 if fault == "oversized" else "[]")
    elif fault == "inventory":
        (tmp_path / "inventory.json").write_text('{"scope_sha256":"wrong","scan":{}}')
    elif fault == "unknown_file":
        (tmp_path / "leftover.tmp").write_text("private incomplete state")
    else:
        record = next(path for path in tmp_path.glob("*.json") if len(path.stem) == 64)
        data = json.loads(record.read_text())
        if fault == "object_scope":
            data["key"] = "benchmarks/foreign/log-history/manifest.json"
            record.write_text(json.dumps(data))
        elif fault == "object_identity":
            data["scope_sha256"] = "e" * 64
            record.write_text(json.dumps(data))
        else:
            record.rename(tmp_path / ("f" * 64 + ".json"))

    with pytest.raises(archive.ArchiveError, match=message):
        run_archive(archive, tmp_path, logs, storage)

    assert storage.objects == original_objects
    assert logs.scan == prior_scans


def test_concurrent_journal_user_cannot_scan_or_upload(tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    journal = UploadJournal(tmp_path, "a" * 64, scoped_input(archive).prefix, 10)
    with journal.locked(), pytest.raises(archive.ArchiveError, match="already in use"):
        run_archive(archive, tmp_path, logs, storage)

    assert logs.scan == 0
    assert storage.objects == {}


def test_changed_chunk_cannot_replace_recorded_version(tmp_path: Path) -> None:
    logs, storage = FakeLogs(), FakeS3()
    logs.events *= 4
    limits = archive.ArchiveLimits(chunk_bytes=512)
    run_archive(archive, tmp_path, logs, storage, limits=limits)
    original_objects = dict(storage.objects)
    logs.events[0]["message"] = "different source content"

    with pytest.raises(archive.ArchiveError, match="content conflict"):
        run_archive(archive, tmp_path, logs, storage, limits=limits)

    assert storage.objects == original_objects


@pytest.mark.parametrize("fault", ["chunk_key", "chunk_ordinal", "event_count", "streams", "scan"])
def test_manifest_semantics_are_checked_after_object_hash(tmp_path: Path, fault: str) -> None:
    logs, storage = FakeLogs(), FakeS3()
    report = run_archive(archive, tmp_path, logs, storage)
    reference = report.reference
    key = (reference.manifest.key, reference.manifest.version_id)
    data = json.loads(storage.objects[key])
    if fault == "chunk_key":
        data["chunks"][0]["object"]["key"] = reference.prefix + "chunks/00000001.json"
    elif fault == "chunk_ordinal":
        data["chunks"][0]["first_ordinal"] = 1
    elif fault == "event_count":
        data["chunks"][0]["event_count"] = 3
    elif fault == "streams":
        data["stream_names"] = ["old", "empty"]
    else:
        data["second_scan"]["event_sha256"] = "f" * 64
    content = encode(data)
    storage.objects[key] = content
    reference = reference.model_copy(
        update={
            "manifest": reference.manifest.model_copy(update={"sha256": digest(content), "size_bytes": len(content)})
        }
    )

    with pytest.raises(archive.ArchiveError, match="manifest (identity|chunk|event)"):
        archive.read_manifest(reference, scoped_input(archive), FakeSession(DESTINATION_ACCOUNT, storage))


@pytest.mark.parametrize("fault", ["identity", "ordinal", "stream", "format", "missing", "digest"])
def test_chunk_and_aggregate_verification_rejects_corruption(tmp_path: Path, fault: str) -> None:
    logs, storage = FakeLogs(), FakeS3()
    report = run_archive(archive, tmp_path, logs, storage)
    session = FakeSession(DESTINATION_ACCOUNT, storage)
    manifest = archive.read_manifest(report.reference, scoped_input(archive), session)
    chunk = manifest.chunks[0]
    key = (chunk.object.key, chunk.object.version_id)
    data = json.loads(storage.objects[key])
    if fault == "identity":
        data["run_id"] = str(manifest.operation_id)
    elif fault == "ordinal":
        data["events"][1]["ordinal"] = 10
    elif fault == "stream":
        data["events"][0]["stream_name"] = "foreign-stream"
    elif fault == "format":
        data = {"unsupported": True}
    elif fault == "digest":
        manifest = manifest.model_copy(
            update={"first_scan": manifest.first_scan.model_copy(update={"event_sha256": "e" * 64})}
        )
    if fault == "missing":
        storage.objects.pop(key)
    else:
        content = encode(data)
        storage.objects[key] = content
        chunk = chunk.model_copy(
            update={"object": chunk.object.model_copy(update={"sha256": digest(content), "size_bytes": len(content)})}
        )
        manifest = manifest.model_copy(update={"chunks": (chunk,)})

    with pytest.raises(archive.ArchiveError, match="chunk|event digest"):
        list(archive.read_events(manifest, scoped_input(archive), session))


def test_reader_does_not_request_foreign_run_object() -> None:
    storage = FakeS3()
    store = archive.ArchiveVersionStore(FakeSession(DESTINATION_ACCOUNT, storage), scoped_input(archive).location)
    reference = ArchiveObject.model_validate(
        {
            "key": f"benchmarks/{RUN_ID}/log-history/{RUN_ID}/v1/manifest.json",
            "version_id": "v1",
            "sha256": "a" * 64,
            "size_bytes": 1,
        }
    ).model_copy(update={"key": "benchmarks/foreign/log-history/manifest.json"})

    with pytest.raises(archive.ArchiveError, match="scope"):
        store.read(reference, 1024)

    assert not any(method == "get" for method, _ in storage.requests)
