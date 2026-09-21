"""Bounded historical log writer and exact-version private format reader.

These synchronous operator helpers do not acquire holds, change database rows,
replay logs, publish public endpoints, or delete any source/destination data.
"""

import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from tracker.aws.log_history_source import FrozenLogSource
from tracker.aws.log_history_store import ArchiveVersionStore, UploadJournal, digest, encode, same_inventory
from tracker.exceptions import TrackerServiceError
from tracker.lifecycle import Verification, unverified
from tracker.runtime.log_history import (
    ArchiveChunk,
    ArchivedLogEvent,
    ArchiveError,
    ArchiveLimits,
    ArchiveLocation,
    ArchiveReport,
    ChunkReference,
    FrozenLogScope,
    LogHistoryManifest,
    LogHistoryReference,
    ScanEvidence,
)


class _ChunkWriter:
    def __init__(
        self,
        scope: FrozenLogScope,
        limits: ArchiveLimits,
        journal: UploadJournal,
        store: ArchiveVersionStore,
        verify: Verification,
    ) -> None:
        self.scope = scope
        self.limits = limits
        self.journal = journal
        self.store = store
        self.verify = verify
        self.events: list[ArchivedLogEvent] = []
        self.chunks: list[ChunkReference] = []
        self.content_bytes = 0

    def _chunk(self, events: tuple[ArchivedLogEvent, ...], ordinal: int) -> ArchiveChunk:
        return ArchiveChunk(
            run_id=self.scope.source.run_id,
            operation_id=self.scope.source_identity.operation_id,
            first_ordinal=ordinal,
            events=events,
        )

    def append(self, event: ArchivedLogEvent) -> None:
        event_bytes = len(encode(event))
        if not self.events:
            self.content_bytes = len(encode(self._chunk((), event.ordinal)))

        extra = event_bytes + bool(self.events)
        if self.events and self.content_bytes + extra > self.limits.chunk_bytes:
            self.flush()
            self.content_bytes = len(encode(self._chunk((), event.ordinal)))
            extra = event_bytes

        if self.content_bytes + extra > self.limits.chunk_bytes:
            raise ArchiveError("archive chunk byte limit exceeded")

        self.events.append(event)
        self.content_bytes += extra

    def flush(self) -> None:
        if not self.events:
            return

        if len(self.chunks) >= self.limits.max_chunks:
            raise ArchiveError("archive chunk count limit exceeded")

        chunk = self._chunk(tuple(self.events), self.events[0].ordinal)
        key = f"{self.scope.prefix}chunks/{len(self.chunks):08d}.json"
        self.verify()
        reference = self.journal.put(self.store, key, encode(chunk), self.limits.chunk_bytes)
        self.chunks.append(
            ChunkReference(object=reference, first_ordinal=chunk.first_ordinal, event_count=len(chunk.events))
        )
        self.events.clear()


def archive_logs(
    scope: FrozenLogScope,
    *,
    source_session: Any,
    destination_session: Any,
    journal_directory: Path,
    limits: ArchiveLimits = ArchiveLimits(),
    staged_scan: ScanEvidence | None = None,
    verify: Verification = unverified,
) -> ArchiveReport:
    """Verify both authorities, scan twice, and publish a manifest last.

    Sessions must use separate, explicitly selected credentials. A durable journal
    directory belongs to exactly this approved scope and must survive restarts.
    A caller that already gated an evidence-only scan stages it here, so the
    published inventory is the one the caller cleared and never a later one.
    A caller holding an operation lock passes its verification, which runs again
    immediately before every object this writes.
    """
    try:
        if source_session is destination_session:
            raise ArchiveError("separate source and destination sessions required")

        source = FrozenLogSource(source_session, scope, limits)
        store = ArchiveVersionStore(destination_session, scope.location)
        scope_sha256 = digest(
            encode({"scope": scope.model_dump(mode="json"), "limits": limits.model_dump(mode="json")})
        )
        journal = UploadJournal(journal_directory, scope_sha256, scope.prefix, limits.max_chunks)
        with journal.locked():
            writer = _ChunkWriter(scope, limits, journal, store, verify)
            streams, first = source.scan(writer.append)
            if staged_scan is not None:
                if not same_inventory(staged_scan, first):
                    raise ArchiveError("frozen source changed between scans")

                first = staged_scan

            journal.remember_inventory(first)
            writer.flush()
            second_streams, second = source.scan(lambda _event: None)
            if streams != second_streams or not same_inventory(first, second):
                raise ArchiveError("frozen source changed between scans")

            journal.require_chunks([chunk.object for chunk in writer.chunks])
            manifest = LogHistoryManifest(
                run_id=scope.source.run_id,
                operation_id=scope.source_identity.operation_id,
                parent_plan_sha256=scope.source_identity.parent_plan_sha256,
                scope_sha256=scope_sha256,
                source_account_id=scope.source_identity.source_aws_account_id,
                source_region=scope.source_identity.region,
                source_group=scope.source.log_group,
                source_principal_arn=source.principal,
                destination=scope.location,
                freeze_evidence_sha256=scope.freeze_evidence_sha256,
                limits=limits,
                stream_names=streams,
                chunks=tuple(writer.chunks),
                first_scan=first,
                second_scan=second,
            )
            manifest_object = journal.manifest
            if manifest_object is None:
                verify()
                manifest_object = journal.put(
                    store, f"{scope.prefix}manifest.json", encode(manifest), limits.manifest_bytes
                )
            else:
                saved_reference = LogHistoryReference(
                    run_id=scope.source.run_id,
                    operation_id=scope.source_identity.operation_id,
                    parent_plan_sha256=scope.source_identity.parent_plan_sha256,
                    manifest=manifest_object,
                )
                saved = read_manifest(saved_reference, scope.location, destination_session)
                observations = {"source_principal_arn", "first_scan", "second_scan"}
                if (
                    saved.model_dump(exclude=observations) != manifest.model_dump(exclude=observations)
                    or not same_inventory(saved.first_scan, first)
                    or not same_inventory(saved.second_scan, second)
                ):
                    raise ArchiveError("saved manifest scope or inventory conflict")

            reference = LogHistoryReference(
                run_id=scope.source.run_id,
                operation_id=scope.source_identity.operation_id,
                parent_plan_sha256=scope.source_identity.parent_plan_sha256,
                manifest=manifest_object,
            )
            return ArchiveReport(
                reference=reference,
                event_count=first.event_count,
                stream_count=first.stream_count,
                chunk_count=len(writer.chunks),
                event_sha256=first.event_sha256,
            )
    except (ArchiveError, TrackerServiceError):
        # The masking below hides provider detail; the tracker's own refusals are not that.
        raise
    except Exception:
        raise ArchiveError("archive operation failed; source must remain intact") from None


def _location(value: ArchiveLocation | FrozenLogScope) -> ArchiveLocation:
    return value.location if isinstance(value, FrozenLogScope) else value


def read_manifest(
    reference: LogHistoryReference,
    destination: ArchiveLocation | FrozenLogScope,
    session: Any,
    store: ArchiveVersionStore | None = None,
) -> LogHistoryManifest:
    """No listing fallback: missing or corrupt declared history is an error."""
    try:
        location = _location(destination)
        if reference.run_id != location.run_id:
            raise ArchiveError("archive run scope mismatch")

        if store is not None and store.location != location:
            raise ArchiveError("archive store scope mismatch")

        store = store or ArchiveVersionStore(session, location)
        manifest = LogHistoryManifest.model_validate_json(store.read(reference.manifest, 16 * 1024 * 1024))
        if (
            manifest.run_id != reference.run_id
            or manifest.operation_id != reference.operation_id
            or manifest.parent_plan_sha256 != reference.parent_plan_sha256
            or manifest.destination != location
            or not same_inventory(manifest.first_scan, manifest.second_scan)
            or reference.manifest.size_bytes > manifest.limits.manifest_bytes
            or manifest.stream_names != tuple(sorted(set(manifest.stream_names)))
            or len(manifest.stream_names) != manifest.first_scan.stream_count
            or len(manifest.stream_names) > manifest.limits.max_streams
            or len(manifest.chunks) > manifest.limits.max_chunks
            or digest(encode(list(manifest.stream_names))) != manifest.first_scan.stream_sha256
        ):
            raise ArchiveError("manifest identity or inventory mismatch")

        ordinal = 0
        for index, chunk in enumerate(manifest.chunks):
            if (
                chunk.object.key != f"{reference.prefix}chunks/{index:08d}.json"
                or chunk.first_ordinal != ordinal
                or chunk.object.size_bytes > manifest.limits.chunk_bytes
            ):
                raise ArchiveError("manifest chunk scope or limit mismatch")
            ordinal += chunk.event_count

        if ordinal != manifest.first_scan.event_count or (
            manifest.first_scan.group_absent and (ordinal or manifest.stream_names)
        ):
            raise ArchiveError("manifest event count mismatch")

        return manifest
    except ArchiveError:
        raise
    except Exception:
        raise ArchiveError("manifest verification failed") from None


def read_chunk(manifest: LogHistoryManifest, index: int, store: ArchiveVersionStore) -> ArchiveChunk:
    """Read one bounded chunk from an already verified manifest."""
    try:
        if store.location != manifest.destination or index < 0:
            raise ArchiveError("chunk destination or index mismatch")

        reference = manifest.chunks[index]
        content = store.read(reference.object, manifest.limits.chunk_bytes)
        chunk = ArchiveChunk.model_validate_json(content)
        if (
            chunk.run_id != manifest.run_id
            or chunk.operation_id != manifest.operation_id
            or chunk.first_ordinal != reference.first_ordinal
            or len(chunk.events) != reference.event_count
        ):
            raise ArchiveError("chunk identity or count mismatch")

        for ordinal, event in enumerate(chunk.events, start=chunk.first_ordinal):
            if event.ordinal != ordinal or event.stream_name not in manifest.stream_names:
                raise ArchiveError("chunk order or stream mismatch")

        return chunk
    except ArchiveError:
        raise
    except (ValidationError, IndexError):
        raise ArchiveError("chunk format verification failed") from None
    except Exception:
        raise ArchiveError("chunk version verification failed") from None


def read_events(
    manifest: LogHistoryManifest, destination: ArchiveLocation | FrozenLogScope, session: Any
) -> Iterator[ArchivedLogEvent]:
    """Iterate without whole-run buffering; verify the aggregate on exhaustion."""
    try:
        location = _location(destination)
        if location != manifest.destination:
            raise ArchiveError("archive destination scope mismatch")

        store = ArchiveVersionStore(session, location)
        checksum = hashlib.sha256()
        for index in range(len(manifest.chunks)):
            chunk = read_chunk(manifest, index, store)
            for event in chunk.events:
                checksum.update(encode(event) + b"\n")
                yield event

        if checksum.hexdigest() != manifest.first_scan.event_sha256:
            raise ArchiveError("archive event digest mismatch")
    except ArchiveError:
        raise
    except Exception:
        raise ArchiveError("archive event verification failed") from None
