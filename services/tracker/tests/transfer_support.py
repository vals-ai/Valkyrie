"""Controlled paired transfer provider for database transaction tests."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from botocore.exceptions import ClientError
from sqlmodel import Session

from tracker.aws.runtime import AWSResources
from tracker.database.models import Benchmark, Org
from tracker.lifecycle import RunScope
from tracker.lifecycle_evidence import DispatchDrain
from tracker.run_purge.locking import database_target
from tracker.run_transfer.contracts import TransferRequest, TransferRun
from tracker.run_transfer.providers import TransferAWSBoundary
from tracker.run_transfer.rows import RowClosure
from tracker.runtime.log_history import ArchiveReport, LogHistoryManifest, LogHistoryReference, ScanEvidence
from tracker.runtime.log_history_reference import ArchiveObject


OBSERVED_DECISION = "e" * 64
OBSERVED_ACQUIRED_AT = datetime(2020, 1, 1, tzinfo=UTC)


class ObservedEventsBoundary(TransferAWSBoundary):
    """Test observed-event transport only; this supplies no production completeness proof.

    The quiet-interval clauses are replaced by a fixed decision so that transport
    tests can use freshly created fake log events.
    """

    def _require_quiet_drained_run(
        self,
        request: TransferRequest,
        run: TransferRun,
        *,
        dispatches: tuple[DispatchDrain, ...],
        acquired_at: datetime,
        scan: ScanEvidence | None = None,
    ) -> datetime:
        return datetime.now(UTC)

    def _require_log_completeness(
        self,
        request: TransferRequest,
        run: TransferRun,
        manifest: LogHistoryManifest,
        *,
        dispatches: tuple[DispatchDrain, ...],
        acquired_at: datetime,
        log_completeness_sha256: str | None,
    ) -> str:
        return OBSERVED_DECISION

    async def archive(
        self,
        request: TransferRequest,
        run: TransferRun,
        *,
        dispatches: tuple[DispatchDrain, ...] = (),
        acquired_at: datetime = OBSERVED_ACQUIRED_AT,
    ) -> ArchiveReport:
        return await super().archive(request, run, dispatches=dispatches, acquired_at=acquired_at)

    async def verify_archive(
        self,
        request: TransferRequest,
        run: TransferRun,
        archive: ArchiveReport,
        *,
        dispatches: tuple[DispatchDrain, ...] = (),
        acquired_at: datetime = OBSERVED_ACQUIRED_AT,
        log_completeness_sha256: str | None = OBSERVED_DECISION,
    ) -> str:
        return await super().verify_archive(
            request,
            run,
            archive,
            dispatches=dispatches,
            acquired_at=acquired_at,
            log_completeness_sha256=log_completeness_sha256,
        )

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
        await super().verify_objects(
            request,
            run,
            source_removed=source_removed,
            source_partial=source_partial,
            archive=archive,
            dispatches=dispatches,
            acquired_at=acquired_at,
            log_completeness_sha256=log_completeness_sha256,
        )

    async def cleanup_logs(
        self,
        request: TransferRequest,
        run: TransferRun,
        archive: ArchiveReport,
        *,
        dispatches: tuple[DispatchDrain, ...] = (),
        acquired_at: datetime = OBSERVED_ACQUIRED_AT,
        log_completeness_sha256: str | None = OBSERVED_DECISION,
    ) -> None:
        await super().cleanup_logs(
            request,
            run,
            archive,
            dispatches=dispatches,
            acquired_at=acquired_at,
            log_completeness_sha256=log_completeness_sha256,
        )


class SecretMetadataSession:
    def __init__(self, metadata: dict[str, dict[str, Any]]) -> None:
        self.metadata = metadata
        self.requested: list[str] = []

    def client(self, service: str, *, region_name: str) -> "SecretMetadataSession":
        assert service == "secretsmanager"
        assert region_name == "us-west-2"
        return self

    def describe_secret(self, *, SecretId: str) -> dict[str, Any]:
        self.requested.append(SecretId)
        if SecretId not in self.metadata:
            raise ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "DescribeSecret")

        return self.metadata[SecretId]


def transfer_request(source: Session, destination: Session, org: Org, run: Benchmark) -> dict[str, Any]:
    resources = AWSResources("us-east-1", "legacy-source", "logs", 7)
    run.arguments = run.arguments.model_copy(
        update={"properties": resources, "sandbox_provider_secret_name": "source-provider"}
    )
    source.add(run)
    source.commit()
    identity = {
        "operation_id": str(uuid4()),
        "parent_plan_sha256": "a" * 64,
        "github_owner_id": 42,
        "org_id": str(org.id),
        "source_aws_account_id": "111111111111",
        "destination_aws_account_id": "222222222222",
        "region": "us-east-1",
        "environment": "test",
        "database_target": database_target(source),
        "run_ids": [str(run.id)],
    }
    return {
        "nonce": str(uuid4()),
        "action": "plan",
        "plan": {
            "source_identity": identity,
            "destination_identity": {
                **identity,
                "region": "us-west-2",
                "database_target": database_target(destination),
            },
            "org_name": org.name,
            "runs": [
                {
                    "source": RunScope(run_id=run.id, original_resources=resources).model_dump(mode="json"),
                    "destination": RunScope(
                        run_id=run.id, original_resources=AWSResources("us-west-2", "vs-test-42-target", "new-logs", 7)
                    ).model_dump(mode="json"),
                    "execution_policy": "history_only",
                    "unmasked_read_authorized": True,
                }
            ],
        },
        "source_host_contract": {
            "contract": "stable-host-lifecycle-v1",
            "deployment_sha256": "b" * 64,
            "host_inventory": ["host-1"],
            "observed_at": datetime.now(UTC).isoformat(),
            "acknowledgement_required_since": (datetime.now(UTC) - timedelta(days=1)).isoformat(),
            "verifier": "local-test",
        },
    }


class FakeTransferBoundary:
    """Isolated orchestration double, not a deployed complete-ingestion provider."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.fail_archive = False
        self.fail_cleanup = False
        self.absent = True

    async def validate(self, request: TransferRequest, run: TransferRun) -> None:
        pass

    async def drain(
        self, request: TransferRequest, run: TransferRun, arguments: dict[str, Any], *, cleanup: bool = False
    ) -> None:
        if not self.absent:
            raise RuntimeError("provider still active")

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
        if archive is not None and log_completeness_sha256 != OBSERVED_DECISION:
            raise RuntimeError("archive acceptance without the persisted completeness decision")

    async def archive(
        self,
        request: TransferRequest,
        run: TransferRun,
        *,
        dispatches: tuple[DispatchDrain, ...] = (),
        acquired_at: datetime = OBSERVED_ACQUIRED_AT,
    ) -> ArchiveReport:
        if self.fail_archive:
            raise RuntimeError("archive failed")
        return ArchiveReport(
            reference=LogHistoryReference(
                run_id=run.source.run_id,
                operation_id=request.plan.source_identity.operation_id,
                parent_plan_sha256=request.plan.source_identity.parent_plan_sha256,
                manifest=ArchiveObject.model_validate(
                    {
                        "key": f"benchmarks/{run.source.run_id}/log-history/{request.plan.source_identity.operation_id}/v1/manifest.json",
                        "version_id": "manifest-version",
                        "sha256": "c" * 64,
                        "size_bytes": 1,
                    }
                ),
            ),
            event_count=2,
            stream_count=2,
            chunk_count=1,
            event_sha256="d" * 64,
        )

    async def verify_archive(
        self,
        request: TransferRequest,
        run: TransferRun,
        archive: ArchiveReport,
        *,
        dispatches: tuple[DispatchDrain, ...] = (),
        acquired_at: datetime = OBSERVED_ACQUIRED_AT,
        log_completeness_sha256: str | None = None,
    ) -> str:
        if log_completeness_sha256 not in {None, OBSERVED_DECISION}:
            raise RuntimeError("persisted completeness decision differs")

        return OBSERVED_DECISION

    async def cleanup_logs(
        self,
        request: TransferRequest,
        run: TransferRun,
        archive: ArchiveReport,
        *,
        dispatches: tuple[DispatchDrain, ...] = (),
        acquired_at: datetime = OBSERVED_ACQUIRED_AT,
        log_completeness_sha256: str | None = OBSERVED_DECISION,
    ) -> None:
        if log_completeness_sha256 != OBSERVED_DECISION:
            raise RuntimeError("log cleanup without the persisted completeness decision")

        if self.fail_cleanup:
            raise RuntimeError("cleanup failed")

    async def portable(self, request: TransferRequest, run: TransferRun, rows: RowClosure) -> None:
        raise RuntimeError("references unresolved")
