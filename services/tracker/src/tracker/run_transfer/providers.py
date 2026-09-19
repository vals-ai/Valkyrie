"""Separate AWS authorities; no historical event replay or secret export."""

import asyncio
import hashlib
import json
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
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
from tracker.run_purge.contracts import ProviderLocator, PurgeRun
from tracker.run_relocation.providers import RelocationAWSBoundary
from tracker.run_transfer.contracts import TransferRequest, TransferRun
from tracker.run_transfer.references import verify_portable_references
from tracker.run_transfer.rows import RowClosure, digest
from tracker.runtime.log_history import ArchiveReport, FrozenLogScope
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

    def _require_log_completeness(self) -> None:
        # Process exit and matching queries do not prove complete CloudWatch ingestion.
        raise LifecycleConflict("Historical log completeness is unproved; transfer remains pending")

    def _sessions(self) -> tuple[Any, Any]:
        if self.source_session is None:
            assert self.source is not None
            self.source_session = self.source.boto3_session()
        if self.destination_session is None:
            assert self.destination is not None
            self.destination_session = self.destination.boto3_session()
        return self.source_session, self.destination_session

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

    async def archive(self, request: TransferRequest, run: TransferRun) -> ArchiveReport:
        self._require_log_completeness()
        source, destination = self._sessions()
        (self.journal / str(request.plan.source_identity.operation_id)).mkdir(parents=True, exist_ok=True, mode=0o700)
        return await asyncio.to_thread(
            archive_logs,
            self._scope(request, run),
            source_session=source,
            destination_session=destination,
            journal_directory=self.journal / str(request.plan.source_identity.operation_id) / str(run.source.run_id),
        )

    async def verify_archive(self, request: TransferRequest, run: TransferRun, archive: ArchiveReport) -> None:
        self._require_log_completeness()
        _, destination = self._sessions()
        scope = self._scope(request, run)
        manifest = await asyncio.to_thread(read_manifest, archive.reference, scope, destination)
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

    async def verify_objects(
        self,
        request: TransferRequest,
        run: TransferRun,
        *,
        source_removed: bool = False,
        source_partial: bool = False,
        archive: ArchiveReport | None = None,
    ) -> None:
        assert self.source is not None and self.destination is not None
        identity = request.plan.source_identity
        history = request.destination_versions
        if archive is not None:
            self._require_log_completeness()
            _, destination = self._sessions()
            manifest = read_manifest(archive.reference, self._scope(request, run), destination)
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

    async def cleanup_logs(self, request: TransferRequest, run: TransferRun, archive: ArchiveReport) -> None:
        await self.verify_archive(request, run, archive)
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
