"""Partial cleanup uses real version verification with an isolated archive/provider double."""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from sqlmodel import Session

from tests.integration.local.database.test_run_transfer import pair as pair
from tests.integration.local.database.test_run_transfer import seed_rows
from tests.integration.local.database.test_transfer_inspection import execute, snapshot
from tests.relocation_support import VersionStore
from tests.transfer_support import OBSERVED_ACQUIRED_AT, OBSERVED_DECISION, FakeTransferBoundary, transfer_request
from tracker.database.models import RunLifecycle
from tracker.lifecycle import LifecycleConflict
from tracker.lifecycle_evidence import DispatchDrain
from tracker.run_transfer import TransferOperator
from tracker.run_transfer.contracts import TransferRequest, TransferRun
from tracker.run_transfer.providers import TransferAWSBoundary
from tracker.run_transfer.rows import digest
from tracker.runtime.log_history import ArchiveReport


class PairedVersions(VersionStore):
    def __init__(self, request: TransferRequest) -> None:
        run = request.plan.runs[0]
        super().__init__(str(run.source.run_id))
        self.source_bucket = run.source.original_resources.s3_bucket
        self.destination_bucket = run.destination.original_resources.s3_bucket
        self.accounts = {self.source_bucket: "111111111111", self.destination_bucket: "222222222222"}
        self.versions = {
            self.source_bucket: [("s2", b'{"value":2}'), ("s1", b'{"value":1}')],
            self.destination_bucket: [("d2", b'{"value":2}'), ("d1", b'{"value":1}')],
        }
        self.fence_statement = {
            "Sid": "ValSmithOwnerMigration" + request.plan.source_identity.operation_id.hex,
            "Effect": "Deny",
            "Principal": "*",
            "Action": ["s3:PutObject", "s3:DeleteObject"],
            "Resource": [f"arn:aws:s3:::{self.source_bucket}/{run.source.object_prefix}*"],
        }

    def bound(self, arguments: dict[str, Any]) -> dict[str, Any]:
        assert arguments["ExpectedBucketOwner"] == self.accounts[arguments["Bucket"]]
        return {**arguments, "ExpectedBucketOwner": "123456789012"}

    async def get_bucket_policy(self, **arguments: Any) -> dict[str, str]:
        return await super().get_bucket_policy(**self.bound(arguments))

    async def list_object_versions(self, **arguments: Any) -> dict[str, Any]:
        return await super().list_object_versions(**self.bound(arguments))

    async def get_object(self, **arguments: Any) -> dict[str, Any]:
        return await super().get_object(**self.bound(arguments))

    async def list_multipart_uploads(self, **arguments: Any) -> dict[str, Any]:
        return await super().list_multipart_uploads(**self.bound(arguments))


class ObjectInspectionBoundary(FakeTransferBoundary):
    """Real paired object checks; archive completeness and providers remain test doubles."""

    def __init__(self, directory: Path, store: PairedVersions) -> None:
        super().__init__(directory)
        source, destination = Mock(), Mock()
        source.s3_client.return_value = store
        destination.s3_client.return_value = store
        self.objects = TransferAWSBoundary(source, destination, directory)
        self.inspection_modes: list[tuple[bool, bool]] = []

    async def verify_objects(
        self,
        request: TransferRequest,
        run: TransferRun,
        *,
        source_removed: bool = False,
        source_partial: bool = False,
        archive: ArchiveReport | None = None,
        dispatches: tuple[DispatchDrain, ...] = (),
        acquired_at: datetime = OBSERVED_ACQUIRED_AT,
        log_completeness_sha256: str | None = OBSERVED_DECISION,
    ) -> None:
        if request.action == "inspect":
            self.inspection_modes.append((source_removed, source_partial))
        options: dict[str, Any] = {"source_partial": True} if source_partial else {}
        await self.objects.verify_objects(
            request,
            run,
            source_removed=source_removed,
            dispatches=dispatches,
            acquired_at=acquired_at,
            log_completeness_sha256=log_completeness_sha256,
            **options,
        )


def imported_versions(
    pair: tuple[Session, Session],
    tmp_path: Path,
) -> tuple[TransferOperator, ObjectInspectionBoundary, PairedVersions, dict[str, Any]]:
    source, destination = pair
    org, run, _ = seed_rows(source, destination)
    payload = transfer_request(source, destination, org, run)
    store = PairedVersions(TransferRequest.model_validate(payload))
    copied_objects: list[dict[str, Any]] = []
    destination_versions: list[dict[str, Any]] = []
    payload["copied_objects"] = copied_objects
    payload["destination_versions"] = destination_versions
    for index, (identifier, content) in enumerate(store.versions[store.source_bucket]):
        assert content is not None
        checksum = hashlib.sha256(content).hexdigest()
        target = identifier.replace("s", "d")
        copied_objects.append(
            {
                "run_id": str(run.id),
                "key": store.key,
                "source_bucket": store.source_bucket,
                "destination_bucket": store.destination_bucket,
                "source_version_id": identifier,
                "destination_version_id": target,
                "is_delete_marker": False,
                "is_current": index == 0,
                "source_sha256": checksum,
                "destination_sha256": checksum,
                "source_size": len(content),
                "destination_size": len(content),
            }
        )
        destination_versions.append(
            {
                "run_id": str(run.id),
                "bucket": store.destination_bucket,
                "key": store.key,
                "version_id": target,
                "is_delete_marker": False,
                "size": len(content),
                "sha256": checksum,
                "is_current": index == 0,
                "provenance": "copied",
            }
        )
    boundary = ObjectInspectionBoundary(tmp_path, store)
    operator = TransferOperator(source, destination, boundary)
    planned = execute(operator, payload, "plan")
    payload["plan"]["runs"][0]["source_rows_sha256"] = planned.runs[0].source_rows_sha256
    execute(operator, payload, "prepare")
    imported = execute(operator, payload, "import")
    plan = TransferRequest.model_validate(payload).plan
    payload["parent_completion"] = {
        "operation_id": str(plan.source_identity.operation_id),
        "parent_plan_sha256": plan.source_identity.parent_plan_sha256,
        "child_plan_sha256": plan.sha256,
        "valsmith_commit_sha256": "e" * 64,
        "object_completion_sha256": "f" * 64,
        "destination_rows_sha256": digest(
            [{"run_id": str(item.run_id), "sha256": item.destination_rows_sha256} for item in imported.runs]
        ),
        "archives_sha256": digest(
            [item.archive.model_dump(mode="json") for item in imported.runs if item.archive is not None]
        ),
    }
    return operator, boundary, store, payload


@pytest.mark.parametrize("remaining", [0, 1, 2])
def test_authorized_inspection_accepts_only_remaining_original_versions_without_mutation(
    pair: tuple[Session, Session],
    tmp_path: Path,
    remaining: int,
) -> None:
    operator, boundary, store, payload = imported_versions(pair, tmp_path)
    store.versions[store.source_bucket] = store.versions[store.source_bucket][2 - remaining :]
    before = snapshot(pair)

    response = execute(operator, payload, "inspect")

    assert response.parent_completion_sha256 == digest(payload["parent_completion"])
    assert response.runs[0].source_phase == "transferred"
    assert boundary.inspection_modes == [(False, True)]
    assert snapshot(pair) == before
    if remaining:
        with pytest.raises(LifecycleConflict, match="cleanup is incomplete"):
            execute(operator, payload, "cleanup")
    else:
        execute(operator, payload, "cleanup")
        assert execute(operator, payload, "inspect").runs[0].source_phase == "transferred_source_retired"
        assert boundary.inspection_modes[-1] == (True, False)


@pytest.mark.parametrize(
    "fault",
    [
        "missing_completion",
        "wrong_completion",
        "source_completion",
        "destination_completion",
        "source_phase",
        "destination_phase",
        "missing_copy_hash",
        "missing_destination_hash",
        "changed_copy_hash",
        "extra_source",
        "changed_source",
        "changed_destination",
        "missing_fence",
        "provider_present",
    ],
)
def test_partial_inspection_refuses_unbound_or_changed_evidence(
    pair: tuple[Session, Session],
    tmp_path: Path,
    fault: str,
) -> None:
    operator, boundary, store, payload = imported_versions(pair, tmp_path)
    store.versions[store.source_bucket] = [("s1", b'{"value":1}')]
    run_id = TransferRequest.model_validate(payload).plan.runs[0].source.run_id
    if fault == "missing_completion":
        payload["parent_completion"] = None
    elif fault == "wrong_completion":
        payload["parent_completion"]["archives_sha256"] = "0" * 64
    elif fault in {
        "source_completion",
        "destination_completion",
        "source_phase",
        "destination_phase",
        "missing_copy_hash",
        "missing_destination_hash",
        "changed_copy_hash",
    }:
        session = pair[1] if fault.startswith("destination") else pair[0]
        record = session.get_one(RunLifecycle, run_id)
        checkpoint = json.loads(record.checkpoint_json or "null")
        if fault.endswith("completion"):
            checkpoint["parent_completion_sha256"] = "0" * 64
        elif fault.endswith("phase"):
            record.phase = checkpoint["phase"] = "prepared"
        elif fault == "missing_copy_hash":
            checkpoint["copied_objects_sha256"] = None
        elif fault == "missing_destination_hash":
            checkpoint["destination_versions_sha256"] = None
        else:
            checkpoint["copied_objects_sha256"] = "0" * 64
        record.checkpoint_json = json.dumps(checkpoint)
        session.add(record)
        session.commit()
    elif fault == "extra_source":
        store.versions[store.source_bucket].append(("unknown", b"foreign"))
    elif fault == "changed_source":
        store.versions[store.source_bucket] = [("s1", b"changed")]
    elif fault == "changed_destination":
        store.versions[store.destination_bucket][0] = ("d2", b"changed")
    elif fault == "missing_fence":
        store.fence_statement = None
    else:
        boundary.absent = False
    before = snapshot(pair)

    with pytest.raises((LifecycleConflict, ValueError, RuntimeError)):
        execute(operator, payload, "inspect")

    assert snapshot(pair) == before
    if fault in {
        "wrong_completion",
        "source_completion",
        "destination_completion",
        "source_phase",
        "destination_phase",
        "missing_copy_hash",
        "missing_destination_hash",
        "changed_copy_hash",
    }:
        assert boundary.inspection_modes == []


def test_unbound_inspection_keeps_complete_source_requirement(pair: tuple[Session, Session], tmp_path: Path) -> None:
    operator, boundary, _, payload = imported_versions(pair, tmp_path)
    payload["parent_completion"] = None

    assert execute(operator, payload, "inspect").parent_completion_sha256 is None
    assert boundary.inspection_modes == [(False, False)]
