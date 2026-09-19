"""Separate AWS authorities; no historical event replay or secret export."""

import asyncio
import hashlib
import json
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import aioboto3
import boto3

from tracker.aws.clients import AWSClientProvider
from tracker.aws.historical_logs import HistoricalLogProvider
from tracker.aws.log_history_archive import archive_logs, read_events, read_manifest
from tracker.aws.log_history_source import FrozenLogSource
from tracker.aws.log_history_store import encode, same_inventory
from tracker.lifecycle import LifecycleConflict
from tracker.lifecycle_evidence import DispatchDrain, validate_host_contract_observation
from tracker.run_purge.contracts import ProviderLocator, PurgeRun
from tracker.run_relocation.providers import RelocationAWSBoundary
from tracker.run_transfer.contracts import TransferRequest, TransferRun
from tracker.run_transfer.references import verify_portable_references
from tracker.run_transfer.rows import RowClosure, digest
from tracker.run_transfer.settings import SETTINGS
from tracker.runtime.log_history import (
    ArchiveLimits,
    ArchiveReport,
    FrozenLogScope,
    LogHistoryManifest,
    ScanEvidence,
)
from tracker.runtime.logs import LogPage, RunLogReference, TaskLogReference
from tracker.storage_migration_exchange import (
    DestinationVersion,
    RelocationPlan,
    RelocationRun,
    TrackerRequest,
)
from tracker.storage_migration_exchange import (
    OperationIdentity as ExchangeIdentity,
)


_CONTRACT_DRAINS = {"held_unclaimed", "verified_finished_contract"}


def _incomplete(clause: str) -> LifecycleConflict:
    return LifecycleConflict(f"Historical log completeness clause {clause} failed; transfer remains pending")


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


class ProfileClients(AWSClientProvider):
    credential_source = "managed"

    def __init__(self, profile: str, region: str) -> None:
        self.profile = profile
        self.region = region
        self.clients: dict[str, Any] = {}

    def with_region(self, region: str) -> "ProfileClients":
        return ProfileClients(self.profile, region)

    def _client_kwargs(self) -> dict[str, Any]:
        return {"region_name": self.region}

    def _client(self, service: str) -> Any:
        """Cache per instance; a process-wide cache would outlive the operation."""
        if service not in self.clients:
            self.clients[service] = self.boto3_session().client(service)

        return self.clients[service]

    def boto3_session(self) -> Any:
        return boto3.Session(profile_name=self.profile, region_name=self.region)

    def s3_client(self) -> Any:
        return cast(Any, aioboto3.Session(profile_name=self.profile, region_name=self.region)).client("s3")

    def sts_client(self) -> Any:
        return self._client("sts")

    def secretsmanager_client(self) -> Any:
        return self._client("secretsmanager")

    def secretsmanager_async_client(self) -> Any:
        return cast(Any, aioboto3.Session(profile_name=self.profile, region_name=self.region)).client("secretsmanager")

    def cloudwatch_logs_client(self) -> Any:
        return self._client("logs")


class _RoutedClient:
    def __init__(self, source: Any, destination: Any, request: TransferRequest, run: TransferRun) -> None:
        self.routes = {
            run.source.original_resources.s3_bucket: (source, request.plan.source_identity.source_aws_account_id),
            run.destination.original_resources.s3_bucket: (
                destination,
                request.plan.source_identity.destination_aws_account_id,
            ),
        }

    def __getattr__(self, name: str) -> Any:
        if name not in {"get_bucket_policy", "get_object", "list_object_versions", "list_multipart_uploads"}:
            raise LifecycleConflict("Unsupported paired object inspection")

        async def call(**arguments: Any) -> Any:
            route = self.routes.get(arguments.get("Bucket", ""))
            if route is None:
                raise LifecycleConflict("Object inspection is outside paired bucket scope")
            client, account = route
            return await getattr(client, name)(**{**arguments, "ExpectedBucketOwner": account})

        return call


class _PairedClients(AWSClientProvider):
    """Paired inspection has no single credential source, so only S3 is available."""

    credential_source = "managed"

    def __init__(
        self, source: AWSClientProvider, destination: AWSClientProvider, request: TransferRequest, run: TransferRun
    ) -> None:
        self.source, self.destination, self.request, self.run = source, destination, request, run

    def _client_kwargs(self) -> dict[str, Any]:
        raise LifecycleConflict("Paired inspection has no single credential source")

    def with_region(self, region: str) -> "_PairedClients":
        if region != self.request.plan.source_identity.region:
            raise LifecycleConflict("Paired source region changed")
        return self

    @asynccontextmanager
    async def s3_client(self) -> AsyncGenerator[Any]:
        async with self.source.s3_client() as source, self.destination.s3_client() as destination:
            yield _RoutedClient(source, destination, self.request, self.run)


class _ArchiveOnly:
    async def fetch(self, reference: RunLogReference | TaskLogReference, **_options: Any) -> LogPage:
        return LogPage([])

    async def stream_task(self, reference: TaskLogReference, **_options: Any) -> AsyncIterator[Any]:
        for event in ():
            yield event


class TransferAWSBoundary:
    def __init__(
        self,
        source: AWSClientProvider | None,
        destination: AWSClientProvider | None,
        journal: Path,
        *,
        source_session: Any = None,
        destination_session: Any = None,
    ) -> None:
        self.source = source
        self.destination = destination
        self.journal = journal
        self.source_session = source_session
        self.destination_session = destination_session

    def _require_quiet_drained_run(
        self,
        request: TransferRequest,
        run: TransferRun,
        *,
        dispatches: tuple[DispatchDrain, ...],
        acquired_at: datetime,
        scan: ScanEvidence | None = None,
    ) -> datetime:
        """Clauses 2, 3, 6 and, with a scan, 4. Clause 1 is proven by `_source_rows`."""
        host = request.source_host_contract
        if host is None:
            raise _incomplete("host_observation")

        try:
            validate_host_contract_observation(host)
        except LifecycleConflict as error:
            raise _incomplete("host_observation") from error

        bound = host.observed_at - SETTINGS.log_quiet_interval
        if any(item.provenance == "pending" for item in dispatches):
            raise _incomplete("dispatch_drain")

        observed = [item.observed_exit_at for item in dispatches if item.observed_exit_at is not None]
        observed += [item.observed_at for item in request.external_host_drains if item.run_id == run.source.run_id]
        if any(item.provenance in _CONTRACT_DRAINS for item in dispatches):
            observed.append(host.acknowledgement_required_since)

        if dispatches and not observed:
            raise _incomplete("dispatch_drain")

        if any(_utc(value) >= bound for value in observed):
            raise _incomplete("dispatch_drain")

        if _utc(acquired_at) >= bound:
            raise _incomplete("hold_quiet_interval")

        if scan is not None and scan.event_count:
            if scan.newest_event_ms is None or scan.newest_ingestion_ms is None:
                raise _incomplete("scan_quiet_interval")

            if max(scan.newest_event_ms, scan.newest_ingestion_ms) >= int(bound.timestamp() * 1000):
                raise _incomplete("scan_quiet_interval")

        return bound

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
        """Quiet-interval policy: a documented risk acceptance, not an ingestion proof."""
        scan = manifest.first_scan
        self._require_quiet_drained_run(request, run, dispatches=dispatches, acquired_at=acquired_at, scan=scan)
        if not same_inventory(scan, manifest.second_scan):
            raise _incomplete("matching_scans")

        decision = digest(
            {
                "quiet_interval_hours": SETTINGS.log_quiet_interval_hours,
                "hold_acquired_at": _utc(acquired_at),
                "dispatches": sorted(
                    (item.model_dump(mode="json") for item in dispatches), key=lambda item: item["dispatch_id"]
                ),
                "first_scan": scan.model_dump(mode="json"),
            }
        )
        if log_completeness_sha256 is not None and log_completeness_sha256 != decision:
            raise _incomplete("persisted_decision")

        return decision

    def _sessions(self) -> tuple[Any, Any]:
        if self.source_session is None:
            assert self.source is not None
            self.source_session = self.source.boto3_session()
        if self.destination_session is None:
            assert self.destination is not None
            self.destination_session = self.destination.boto3_session()
        return self.source_session, self.destination_session

    def _scan_evidence(self, source: Any, scope: FrozenLogScope) -> ScanEvidence:
        """Evidence-only scan; it writes nothing, so a failed clause leaves no archive prefix."""
        _, evidence = FrozenLogSource(source, scope, ArchiveLimits()).scan(lambda _event: None)
        return evidence

    def _scope(self, request: TransferRequest, run: TransferRun) -> FrozenLogScope:
        return FrozenLogScope(
            source_identity=request.plan.source_identity,
            destination_identity=request.plan.destination_identity,
            source=run.source,
            destination=run.destination,
            freeze_evidence_sha256=digest(
                {"child_plan_sha256": request.plan.sha256, "source_rows_sha256": run.source_rows_sha256}
            ),
            unmasked_read_authorized=run.unmasked_read_authorized,
        )

    async def validate(self, request: TransferRequest, run: TransferRun) -> None:
        assert self.source is not None and self.destination is not None
        # Validation does not resolve this dummy locator; real provider proof uses the saved locator below.
        provider = ProviderLocator(kind="daytona", secret_name="validation-only")
        await RelocationAWSBoundary(self.source).validate_source(
            request.plan.source_identity, PurgeRun(scope=run.source, provider=provider)
        )
        destination_identity = request.plan.destination_identity.model_copy(
            update={"source_aws_account_id": request.plan.destination_identity.destination_aws_account_id}
        )
        await RelocationAWSBoundary(self.destination).validate(
            destination_identity, PurgeRun(scope=run.destination, provider=provider)
        )

        if request.action == "import":
            await self.verify_source_fence(request, run)

    async def verify_source_fence(self, request: TransferRequest, run: TransferRun) -> None:
        assert self.source is not None
        identity = request.plan.source_identity
        bucket = run.source.original_resources.s3_bucket
        async with self.source.s3_client() as client:
            response = await client.get_bucket_policy(Bucket=bucket, ExpectedBucketOwner=identity.source_aws_account_id)
        policy = json.loads(response["Policy"])
        sid = "ValSmithOwnerMigration" + identity.operation_id.hex
        matching = [item for item in policy.get("Statement", []) if item.get("Sid") == sid]
        if len(matching) != 1:
            raise LifecycleConflict("Exact source transfer fence is missing")
        statement = dict(matching[0])
        resources = statement.pop("Resource", None)
        if (
            statement != {"Sid": sid, "Effect": "Deny", "Principal": "*", "Action": ["s3:PutObject", "s3:DeleteObject"]}
            or not isinstance(resources, list)
            or f"arn:aws:s3:::{bucket}/{run.source.object_prefix}*" not in resources
        ):
            raise LifecycleConflict("Exact source transfer fence differs")

    async def drain(
        self, request: TransferRequest, run: TransferRun, arguments: dict[str, Any], *, cleanup: bool = False
    ) -> None:
        assert self.source is not None
        locator = arguments.get("sandbox_provider_secret_name")
        if not isinstance(locator, str) or not locator:
            raise LifecycleConflict("Exact saved source provider secret locator is missing")

        kind = arguments.get("sandbox_provider")
        if not isinstance(kind, str) or not kind:
            raise LifecycleConflict("Exact saved source provider kind is missing")

        provider_run = PurgeRun(scope=run.source, provider=ProviderLocator(kind=kind, secret_name=locator))
        boundary = RelocationAWSBoundary(self.source)
        if cleanup:
            await boundary.cleanup_sandboxes(provider_run)
        await boundary.verify_absence(provider_run)
        await boundary.verify_absence(provider_run)

    async def archive(
        self,
        request: TransferRequest,
        run: TransferRun,
        *,
        dispatches: tuple[DispatchDrain, ...],
        acquired_at: datetime,
    ) -> ArchiveReport:
        # Refuse on the inputs the caller already holds, before a full source traversal.
        self._require_quiet_drained_run(request, run, dispatches=dispatches, acquired_at=acquired_at)
        source, destination = self._sessions()
        scope = self._scope(request, run)
        evidence = await asyncio.to_thread(self._scan_evidence, source, scope)
        self._require_quiet_drained_run(request, run, dispatches=dispatches, acquired_at=acquired_at, scan=evidence)
        (self.journal / str(request.plan.source_identity.operation_id)).mkdir(parents=True, exist_ok=True, mode=0o700)
        return await asyncio.to_thread(
            archive_logs,
            scope,
            source_session=source,
            destination_session=destination,
            journal_directory=self.journal / str(request.plan.source_identity.operation_id) / str(run.source.run_id),
        )

    async def verify_archive(
        self,
        request: TransferRequest,
        run: TransferRun,
        archive: ArchiveReport,
        *,
        dispatches: tuple[DispatchDrain, ...],
        acquired_at: datetime,
        log_completeness_sha256: str | None,
    ) -> str:
        self._require_quiet_drained_run(request, run, dispatches=dispatches, acquired_at=acquired_at)
        _, destination = self._sessions()
        scope = self._scope(request, run)
        manifest = await asyncio.to_thread(read_manifest, archive.reference, scope, destination)
        decision = self._require_log_completeness(
            request,
            run,
            manifest,
            dispatches=dispatches,
            acquired_at=acquired_at,
            log_completeness_sha256=log_completeness_sha256,
        )
        if (
            manifest.freeze_evidence_sha256 != scope.freeze_evidence_sha256
            or manifest.source_region != request.plan.source_identity.region
            or manifest.source_account_id != request.plan.source_identity.source_aws_account_id
            or manifest.source_group != run.source.log_group
            or (
                manifest.first_scan.event_count,
                manifest.first_scan.stream_count,
                len(manifest.chunks),
                manifest.first_scan.event_sha256,
            )
            != (archive.event_count, archive.stream_count, archive.chunk_count, archive.event_sha256)
        ):
            raise LifecycleConflict("Archive receipt differs from exact manifest and scope")
        checksum = hashlib.sha256()
        expected_count = 0
        for event in read_events(manifest, scope, destination):
            checksum.update(encode(event) + b"\n")
            expected_count += 1
        if expected_count != archive.event_count or checksum.hexdigest() != archive.event_sha256:
            raise LifecycleConflict("Archive full event proof differs")
        reader = HistoricalLogProvider(archive.reference, scope.location, destination, _ArchiveOnly(), terminal=True)
        cursor = None
        seen: set[str] = set()
        count = 0
        while True:
            page = await reader.fetch(RunLogReference(run.source.run_id), cursor=cursor)
            count += len(page.events)
            cursor = page.next_cursor
            if cursor is None:
                break
            if cursor in seen:
                raise LifecycleConflict("Historical reader pagination repeated")
            seen.add(cursor)
        if count != archive.event_count:
            raise LifecycleConflict("Actual historical log reader count differs")

        return decision

    async def verify_objects(
        self,
        request: TransferRequest,
        run: TransferRun,
        *,
        source_removed: bool = False,
        source_partial: bool = False,
        archive: ArchiveReport | None = None,
        dispatches: tuple[DispatchDrain, ...],
        acquired_at: datetime,
        log_completeness_sha256: str | None,
    ) -> None:
        assert self.source is not None and self.destination is not None
        identity = request.plan.source_identity
        history = request.destination_versions
        if archive is not None:
            if log_completeness_sha256 is None:
                raise _incomplete("persisted_decision")

            self._require_quiet_drained_run(request, run, dispatches=dispatches, acquired_at=acquired_at)
            _, destination = self._sessions()
            manifest = read_manifest(archive.reference, self._scope(request, run), destination)
            self._require_log_completeness(
                request,
                run,
                manifest,
                dispatches=dispatches,
                acquired_at=acquired_at,
                log_completeness_sha256=log_completeness_sha256,
            )
            history += tuple(
                DestinationVersion(
                    run_id=run.source.run_id,
                    bucket=run.destination.original_resources.s3_bucket,
                    key=item.key,
                    version_id=item.version_id,
                    is_delete_marker=False,
                    size=item.size_bytes,
                    sha256=item.sha256,
                    is_current=True,
                    provenance="existing",
                )
                for item in (archive.reference.manifest, *(chunk.object for chunk in manifest.chunks))
            )
        projection_identity = identity.model_dump(mode="json")
        projection_identity["destination_aws_account_id"] = identity.source_aws_account_id
        projection_run = RelocationRun.model_validate(
            {
                "scope": run.source.model_dump(mode="json"),
                "destination_resources": run.destination.original_resources.__dict__,
                "expected_label": None,
                "execution_policy": run.execution_policy,
                "execution_arguments_sha256": run.source_rows_sha256,
                "transformations": [item.model_dump(mode="json") for item in run.transformations],
            }
        )
        # Only the read-only version verifier is reused. Its account routing is bound above;
        # the same-account operator and its schema remain unchanged.
        projected = TrackerRequest.model_validate(
            {
                **{
                    key: value
                    for key, value in projection_identity.items()
                    if key not in {"operation_id", "parent_plan_sha256"}
                },
                "action": "inspect",
                "nonce": request.nonce,
                "plan": None,
                "copied_objects": [item.model_dump(mode="json") for item in request.copied_objects],
                "destination_versions": [item.model_dump(mode="json") for item in history],
            }
        )
        plan = RelocationPlan.model_construct(
            identity=ExchangeIdentity.model_validate(projection_identity), runs=(projection_run,)
        )
        projected = projected.model_copy(update={"plan": plan})
        await RelocationAWSBoundary(_PairedClients(self.source, self.destination, request, run)).verify_objects(
            projected, projection_run, source_removed=source_removed, source_partial=source_partial
        )

    async def cleanup_logs(
        self,
        request: TransferRequest,
        run: TransferRun,
        archive: ArchiveReport,
        *,
        dispatches: tuple[DispatchDrain, ...],
        acquired_at: datetime,
        log_completeness_sha256: str | None,
    ) -> None:
        if log_completeness_sha256 is None:
            raise _incomplete("persisted_decision")

        await self.verify_archive(
            request,
            run,
            archive,
            dispatches=dispatches,
            acquired_at=acquired_at,
            log_completeness_sha256=log_completeness_sha256,
        )
        source, destination = self._sessions()
        scope = self._scope(request, run)
        manifest = read_manifest(archive.reference, scope, destination)
        frozen = FrozenLogSource(source, scope, manifest.limits)
        _, inventory = frozen.scan(lambda _event: None)
        if inventory.group_absent:
            return
        if not same_inventory(inventory, manifest.first_scan):
            raise LifecycleConflict("Source logs changed after archive; cleanup refused")
        client = source.client("logs", region_name=request.plan.source_identity.region)
        await asyncio.to_thread(client.delete_log_group, logGroupName=run.source.log_group)
        _, after = frozen.scan(lambda _event: None)
        if not after.group_absent:
            raise LifecycleConflict("Source log deletion remains incomplete")

    async def portable(self, request: TransferRequest, run: TransferRun, rows: RowClosure) -> None:
        _, destination = self._sessions()
        await asyncio.to_thread(
            verify_portable_references,
            rows,
            destination,
            request.plan.destination_identity.destination_aws_account_id,
            request.plan.destination_identity.region,
        )
