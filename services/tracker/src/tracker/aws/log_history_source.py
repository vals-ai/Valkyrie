"""Read complete frozen CloudWatch inventories without replay or deduplication."""

import hashlib
from collections.abc import Callable, Iterator
from typing import Any

from tracker.aws.log_history_store import digest, encode, verified_client
from tracker.runtime.log_history import ArchivedLogEvent, ArchiveError, ArchiveLimits, FrozenLogScope, ScanEvidence


class FrozenLogSource:
    def __init__(self, session: Any, scope: FrozenLogScope, limits: ArchiveLimits) -> None:
        self.scope = scope
        self.limits = limits
        self.client, self.principal = verified_client(
            session, "logs", scope.source_identity.source_aws_account_id, scope.source_identity.region
        )

    def _pages(self, method: Callable[..., Any], **request: Any) -> Iterator[dict[str, Any]]:
        tokens: set[str] = set()
        for _ in range(self.limits.max_pages):
            response = method(**request)
            yield response
            token = response.get("nextToken")
            if token is None:
                return

            if not isinstance(token, str) or not token or token in tokens:
                raise ArchiveError("source pagination did not complete")

            tokens.add(token)
            request["nextToken"] = token

        raise ArchiveError("source page limit exceeded")

    def scan(self, consume: Callable[[ArchivedLogEvent], None]) -> tuple[tuple[str, ...], ScanEvidence]:
        group = self.scope.source.log_group
        found = False
        group_pages = 0
        for page in self._pages(self.client.describe_log_groups, logGroupNamePrefix=group, limit=50):
            group_pages += 1
            groups = page["logGroups"]
            if len(groups) > 50:
                raise ArchiveError("source group page limit exceeded")

            for item in groups:
                if item["logGroupName"] != group:
                    continue

                arn = item["arn"].split(":", 5)
                expected = self.scope.source_identity
                if (
                    len(arn) != 6
                    or arn[2] != "logs"
                    or arn[3] != expected.region
                    or arn[4] != expected.source_aws_account_id
                    or arn[5] != f"log-group:{group}:*"
                    or found
                ):
                    raise ArchiveError("source group identity mismatch")

                found = True

        names: set[str] = set()
        stream_pages = 0
        if found:
            for page in self._pages(
                self.client.describe_log_streams, logGroupName=group, orderBy="LogStreamName", limit=50
            ):
                stream_pages += 1
                streams = page["logStreams"]
                if len(streams) > 50:
                    raise ArchiveError("source stream page limit exceeded")

                for stream in streams:
                    name = stream["logStreamName"]
                    if not isinstance(name, str) or not name or len(name.encode()) > 512 or name in names:
                        raise ArchiveError("source stream inventory is invalid")

                    names.add(name)
                    if len(names) > self.limits.max_streams:
                        raise ArchiveError("source stream limit exceeded")

        event_pages = 0
        event_count = 0
        newest_event_ms: int | None = None
        newest_ingestion_ms: int | None = None
        checksum = hashlib.sha256()
        if found:
            for page in self._pages(self.client.filter_log_events, logGroupName=group, unmask=True, limit=10_000):
                event_pages += 1
                events = page["events"]
                if len(events) > 10_000:
                    raise ArchiveError("source event page limit exceeded")

                for item in events:
                    if set(item) != {"timestamp", "ingestionTime", "message", "eventId", "logStreamName"}:
                        raise ArchiveError("unsupported source event fields")

                    event = ArchivedLogEvent(
                        ordinal=event_count,
                        timestamp=item["timestamp"],
                        ingestion_time=item["ingestionTime"],
                        message=item["message"],
                        event_id=item["eventId"],
                        stream_name=item["logStreamName"],
                    )
                    if event.stream_name not in names:
                        raise ArchiveError("event stream missing from frozen inventory")

                    content = encode(event)
                    if len(content) > self.limits.chunk_bytes:
                        raise ArchiveError("source event byte limit exceeded")

                    checksum.update(content + b"\n")
                    consume(event)
                    event_count += 1
                    newest_event_ms = (
                        event.timestamp if newest_event_ms is None else max(newest_event_ms, event.timestamp)
                    )
                    newest_ingestion_ms = (
                        event.ingestion_time
                        if newest_ingestion_ms is None
                        else max(newest_ingestion_ms, event.ingestion_time)
                    )

        inventory = tuple(sorted(names))
        evidence = ScanEvidence(
            group_absent=not found,
            group_pages=group_pages,
            stream_pages=stream_pages,
            event_pages=event_pages,
            stream_count=len(inventory),
            event_count=event_count,
            stream_sha256=digest(encode(list(inventory))),
            event_sha256=checksum.hexdigest(),
            newest_event_ms=newest_event_ms,
            newest_ingestion_ms=newest_ingestion_ms,
        )

        return inventory, evidence
