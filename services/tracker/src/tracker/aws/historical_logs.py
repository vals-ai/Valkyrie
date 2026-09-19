"""Bounded merge of verified immutable history and destination retry logs."""

import asyncio
import base64
import hashlib
import json
from collections.abc import AsyncIterator
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from tracker.aws.cloudwatch_logs import epoch_milliseconds, run_stream_task_ids, task_log_stream_names
from tracker.aws.log_history_archive import read_chunk, read_manifest
from tracker.aws.log_history_store import ArchiveVersionStore
from tracker.aws.runtime import AWSRuntime
from tracker.runtime.log_history import ArchiveLocation, LogHistoryManifest, LogHistoryReference
from tracker.runtime.logs import LogEvent, LogPage, LogProvider, LogProviderError, RunLogReference, TaskLogReference

_PAGE_SIZE = 1000
_READ_BUDGET = 16
Position = Annotated[int, Field(ge=0, strict=True)]
Identity = Annotated[str, Field(pattern="^[0-9a-f]{64}$")]


class _Cursor(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: Literal[1] = 1
    binding: str
    chunk: Position = 0
    offset: Position = 0
    live_token: str | None = None
    live_offset: Position = 0
    live_event: Identity | None = None
    live_done: bool = False

    @model_validator(mode="after")
    def _bind_live_position(self) -> "_Cursor":
        if (self.live_offset > 0) != (self.live_event is not None):
            raise ValueError("live position and emitted event identity must agree")

        return self

    def encode(self) -> str:
        return base64.urlsafe_b64encode(self.model_dump_json().encode()).decode()


def _binding(
    history: LogHistoryReference,
    reference: RunLogReference | TaskLogReference,
    query: str | None,
    start: datetime | None,
    end: datetime | None,
) -> str:
    value = [
        history.model_dump(mode="json"),
        asdict(reference),
        query,
        epoch_milliseconds(start) if start else None,
        epoch_milliseconds(end) if end else None,
    ]
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _sort_key(event: LogEvent) -> tuple[datetime, datetime]:
    return event.timestamp, event.ingestion_time or event.timestamp


def _event_identity(event: LogEvent) -> str:
    value = [
        epoch_milliseconds(event.timestamp),
        epoch_milliseconds(event.ingestion_time) if event.ingestion_time else None,
        event.event_id,
        event.message,
    ]
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


class _ArchivePage:
    def __init__(
        self,
        manifest: LogHistoryManifest,
        store: ArchiveVersionStore,
        position: _Cursor,
        reference: RunLogReference | TaskLogReference,
        query: str | None,
        start: datetime | None,
        end: datetime | None,
    ) -> None:
        self.manifest = manifest
        self.store = store
        self.position = position
        self.events = None
        self.loaded_index = -1
        self.reads = 0
        self.blocked = False
        self.query = query
        self.start = epoch_milliseconds(start) if start else None
        self.end = epoch_milliseconds(end) if end else None
        self.streams = task_log_stream_names(reference) if isinstance(reference, TaskLogReference) else None
        self.task_id = reference.task_id if isinstance(reference, TaskLogReference) else None
        self.task_ids = run_stream_task_ids(reference) if isinstance(reference, RunLogReference) else {}

    async def peek(self) -> LogEvent | None:
        while self.position.chunk < len(self.manifest.chunks):
            if self.loaded_index != self.position.chunk:
                if self.reads >= _READ_BUDGET:
                    self.blocked = True
                    return None

                chunk = await asyncio.to_thread(read_chunk, self.manifest, self.position.chunk, self.store)
                self.events = chunk.events
                self.loaded_index = self.position.chunk
                self.reads += 1

            assert self.events is not None
            if self.position.offset > len(self.events):
                raise LogProviderError("Invalid archive cursor position")

            if self.position.offset == len(self.events):
                self.position.chunk += 1
                self.position.offset = 0
                continue

            event = self.events[self.position.offset]
            if (
                (self.streams is not None and event.stream_name not in self.streams)
                or (self.query is not None and self.query not in event.message)
                or (self.start is not None and event.timestamp < self.start)
                or (self.end is not None and event.timestamp > self.end)
            ):
                self.position.offset += 1
                continue

            return LogEvent(
                timestamp=datetime.fromtimestamp(event.timestamp / 1000, UTC),
                ingestion_time=datetime.fromtimestamp(event.ingestion_time / 1000, UTC),
                event_id=event.event_id,
                message=event.message,
                task_id=self.task_id or self.task_ids.get(event.stream_name),
            )
        return None


class _LivePage:
    def __init__(
        self,
        provider: LogProvider,
        position: _Cursor,
        reference: RunLogReference | TaskLogReference,
        query: str | None,
        start: datetime | None,
        end: datetime | None,
    ) -> None:
        self.provider = provider
        self.position = position
        self.reference = reference
        self.query = query
        self.start = start
        self.end = end
        self.page: LogPage | None = None
        self.reads = 0
        self.blocked = False
        self.tokens: set[str] = set()

    def _require_resumed_page(self) -> None:
        """A re-read page must still hold the exact event this cursor last emitted."""
        assert self.page is not None
        if self.position.live_event is None:
            return

        if self.position.live_offset > len(self.page.events):
            raise LogProviderError("Invalid live cursor position")

        if _event_identity(self.page.events[self.position.live_offset - 1]) != self.position.live_event:
            raise LogProviderError("Live log page changed between requests")

    async def peek(self) -> LogEvent | None:
        while not self.position.live_done:
            if self.page is None:
                if self.reads >= _READ_BUDGET:
                    self.blocked = True
                    return None

                self.page = await self.provider.fetch(
                    self.reference,
                    query=self.query,
                    start_time=self.start,
                    end_time=self.end,
                    cursor=self.position.live_token,
                    limit=_PAGE_SIZE,
                )
                self.reads += 1
                self._require_resumed_page()

            if self.position.live_offset < len(self.page.events):
                return self.page.events[self.position.live_offset]

            token = self.page.next_cursor
            self.position.live_offset = 0
            self.position.live_event = None
            self.page = None
            if token is None:
                self.position.live_done = True
                return None

            if token in self.tokens or token == self.position.live_token:
                raise LogProviderError("Live log cursor did not advance")

            self.tokens.add(token)
            self.position.live_token = token
        return None


class HistoricalLogProvider(LogProvider):
    def __init__(
        self,
        history: LogHistoryReference,
        location: ArchiveLocation,
        session: Any,
        live: LogProvider,
        *,
        terminal: bool,
    ) -> None:
        self.history = history
        self.location = location
        self.session = session
        self.live = live
        self.terminal = terminal

    async def _open(self) -> tuple[LogHistoryManifest, ArchiveVersionStore]:
        def open_archive() -> tuple[LogHistoryManifest, ArchiveVersionStore]:
            return read_manifest(self.history, self.location, self.session), ArchiveVersionStore(
                self.session, self.location
            )

        try:
            return await asyncio.to_thread(open_archive)
        except Exception:
            raise LogProviderError("Historical archive verification failed") from None

    def _position(
        self,
        reference: RunLogReference | TaskLogReference,
        query: str | None,
        start: datetime | None,
        end: datetime | None,
        cursor: str | None,
    ) -> _Cursor:
        if reference.run_id != self.history.run_id or reference.run_id != self.location.run_id:
            raise LogProviderError("Historical archive run mismatch")

        binding = _binding(self.history, reference, query, start, end)
        if cursor is None:
            return _Cursor(binding=binding)

        try:
            if len(cursor) > 32768:
                raise ValueError

            position = _Cursor.model_validate_json(base64.b64decode(cursor, altchars=b"-_", validate=True))
            if position.binding != binding:
                raise ValueError

            return position
        except (ValueError, ValidationError):
            raise LogProviderError("Invalid historical log cursor") from None

    async def fetch(
        self,
        reference: RunLogReference | TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        cursor: str | None = None,
        limit: int = 1000,
    ) -> LogPage:
        if not 1 <= limit <= 10000:
            raise LogProviderError("Invalid log page limit")

        position = self._position(reference, query, start_time, end_time, cursor)
        manifest, store = await self._open()
        if position.chunk > len(manifest.chunks) or (position.chunk == len(manifest.chunks) and position.offset):
            raise LogProviderError("Invalid archive cursor position")

        archive = _ArchivePage(manifest, store, position, reference, query, start_time, end_time)
        live = _LivePage(self.live, position, reference, query, start_time, end_time)
        events: list[LogEvent] = []
        try:
            while len(events) < limit:
                old = await archive.peek()
                current = await live.peek()
                if archive.blocked or live.blocked:
                    break

                if old is None and current is None:
                    return LogPage(events)

                if old is not None and (current is None or _sort_key(old) <= _sort_key(current)):
                    events.append(old)
                    position.offset += 1
                elif current is not None:
                    events.append(current)
                    position.live_offset += 1
                    position.live_event = _event_identity(current)
        except LogProviderError:
            raise
        except Exception:
            raise LogProviderError("Historical archive verification failed") from None

        return LogPage(events, position.encode())

    async def stream_task(
        self,
        reference: TaskLogReference,
        *,
        query: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        poll_interval: float = 1.0,
    ) -> AsyncIterator[LogEvent]:
        self._position(reference, query, start_time, end_time, None)
        manifest, store = await self._open()
        position = _Cursor(binding="stream")
        try:
            while position.chunk < len(manifest.chunks):
                archive = _ArchivePage(manifest, store, position, reference, query, start_time, end_time)
                while (event := await archive.peek()) is not None:
                    position.offset += 1
                    yield event
        except LogProviderError:
            raise
        except Exception:
            raise LogProviderError("Historical archive verification failed") from None

        # A terminal run has no future writes. A finite end also completes absent groups.
        live_end = end_time
        if self.terminal:
            now = datetime.now(UTC) - timedelta(milliseconds=1)
            live_end = min(live_end, now) if live_end else now

        async for event in self.live.stream_task(
            reference, query=query, start_time=start_time, end_time=live_end, poll_interval=poll_interval
        ):
            yield event


def historical_log_reader(
    runtime: AWSRuntime, run_id: UUID, org_id: UUID, history: LogHistoryReference, live: LogProvider, *, terminal: bool
) -> HistoricalLogProvider:
    """Resolve archive ownership from the saved runtime and verified bucket tags."""
    try:
        session = runtime.clients.boto3_session()
        region = runtime.resources.region
        identity = session.client("sts", region_name=region).get_caller_identity()
        account = identity["Account"]
        if runtime.expected_bucket_owner is not None and runtime.expected_bucket_owner != account:
            raise ValueError("account mismatch")

        storage = session.client("s3", region_name=region)
        tags = {
            item["Key"]: item["Value"]
            for item in storage.get_bucket_tagging(Bucket=runtime.resources.s3_bucket, ExpectedBucketOwner=account)[
                "TagSet"
            ]
        }
        if tags.get("valsmith:valkyrie-org-id") != str(org_id):
            raise ValueError("organization mismatch")

        location = ArchiveLocation(
            run_id=run_id,
            org_id=org_id,
            account_id=account,
            github_owner_id=int(tags["valsmith:owner-account-id"]),
            environment=tags["valsmith:environment"],
            region=region,
            bucket=runtime.resources.s3_bucket,
        )
        ArchiveVersionStore(session, location)
        return HistoricalLogProvider(history, location, session, live, terminal=terminal)
    except Exception:
        raise LogProviderError("Historical archive authority verification failed") from None
