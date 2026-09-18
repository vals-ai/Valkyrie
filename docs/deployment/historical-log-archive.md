# Historical log archive, version 1

This module stores retained CloudWatch history in private, versioned S3 objects.
It does not replay events into CloudWatch. It does not modify database rows,
acquire run holds, remove source data, or compose the public log reader.

## Transfer caller contract (Task 2)

Call the synchronous `tracker.aws.log_history_archive.archive_logs` with:

- A validated `FrozenLogScope`. It binds paired local `OperationIdentity` and
  `RunScope` values, the same run and operation UUIDs, GitHub owner, organization,
  environment, source and destination accounts, parent plan SHA-256, and separate
  database targets and regions. Saved source/destination locations are explicit.
- `freeze_evidence_sha256`, a digest of the transfer operator's durable proof of
  current operation-owned holds, terminal state, positive process drain, and
  absent external writers. This field is an attestation, not a provider check.
  The operator must verify the proof before every apply/resume call and retain
  both holds until later transfer verification. A matching second scan alone
  cannot prove that a live writer has stopped.
- `unmasked_read_authorized=True`. Every event request sets `unmask=True`.
  The source role must have the required unmask authority; a denied request
  leaves the archive unresolved. There is no masked fallback.
- Separate source and destination boto3-compatible sessions with explicit
  credentials. Each session verifies STS account identity and service region.
  The destination bucket must have versioning enabled, enforced bucket ownership,
  the expected region, and matching `valsmith:owner-account-id`,
  `valsmith:valkyrie-org-id`, `valsmith:environment`, and `valsmith:backup=true` tags.
- An exclusive journal directory on a durable local filesystem. Its parent must
  already exist. Preserve this directory across retries and host replacement.
  Do not use a temporary volume for an actual transfer.
- Optional reviewed `ArchiveLimits`. They are bound to the journal scope digest.

Include the exact archive prefix in the approved parent object plan before apply:

```text
benchmarks/<run UUID>/log-history/<operation UUID>/v1/
  chunks/00000000.json
  chunks/00000001.json
  manifest.json
```

The source scan uses exact-name matching after paginated `DescribeLogGroups`,
then exact-group `DescribeLogStreams` and unfiltered `FilterLogEvents`. No time
window or task-stream filter is used. Every continuation token is followed,
including empty pages. Token absence completes a scan; token cycles and limits
fail. The group ARN must match the expected source account, region, and exact
name. An absent exact group is recorded only after a complete group inventory;
two scans must agree on absence. A `ResourceNotFoundException` during a stream
or event read is a failure, not absence evidence.

Streams are stored as a sorted inventory, including empty and unknown streams.
Events retain the source scan order and every occurrence, including repeated
identical IDs/messages. A second complete scan must match stream names, event
count, and the SHA-256 of the ordered full event records. Scan page counts are
recorded separately; page layout can change without changing content. The
manifest is written only after this comparison. Stream timestamps are not used
as completeness watermarks.

`ArchiveReport` contains the typed reference, counts, and ordered event digest.
It contains no messages or provider error text. The manifest and chunks are
private customer history, not report payloads. Provider failure messages are
fixed text. No archive helper deletes objects or logs. The transfer operator
must preserve source data until exact-version verification, database transfer,
and the actual historical read path have all passed.

## Wire format and exact versions (Task 3)

Types are in `tracker.runtime.log_history`. Objects use uncompressed UTF-8 JSON,
with sorted keys, compact separators, no ASCII escaping, and no non-finite
numbers. Compression and content encodings other than identity are rejected.
Thus the encoded and decompressed byte limits are identical.

`LogHistoryReference` contains `format_version=1`, `run_id`, `operation_id`,
`parent_plan_sha256`, and `manifest: ArchiveObject`. Each `ArchiveObject` has
`key`, nonempty/non-null `version_id`, `sha256` (lowercase hex of exact stored
bytes), and `size_bytes`. The manifest key must equal the exact prefix above
plus `manifest.json`. The reference is nullable customer history when Task 3
adds its database field; never copy it into the minimal lifecycle tombstone.

Each `ArchiveChunk` contains `format_version=1`, `run_id`, `operation_id`,
`first_ordinal`, and `events`. Each event has these exact fields:

```json
{"ordinal":0,"timestamp":1,"ingestion_time":2,"message":"example","event_id":"event-identity","stream_name":"original-stream"}
```

Timestamp and ingestion time are original integer milliseconds. `ordinal` is
zero-based across the full run scan. No sorting or deduplication changes the
source order. Source event fields outside the supported CloudWatch event shape
are rejected rather than discarded. The ordered event digest hashes each
canonical event JSON record followed by one newline.

`LogHistoryManifest` contains the run/operation/parent identity, complete scoped
input digest, source account/region/exact group/principal ARN, typed destination
`ArchiveLocation`, freeze evidence digest, unmasked-read flag, encoding, limits,
sorted stream inventory, and ordered `ChunkReference` values. Each chunk
reference contains its exact `ArchiveObject`, first ordinal, and event count.
`first_scan` and `second_scan` each contain group absence, group/stream/event page
counts, stream/event counts, and stream/event digests. The stream digest hashes
the canonical JSON list of sorted stream names.

The helpers are synchronous. Async reader composition must run blocking AWS
work outside the event loop, for example with `asyncio.to_thread`.

Reader calls:

1. Build `ArchiveLocation` from validated saved destination resources and the
   resolved owner/org/environment/account/run scope. It must match the manifest.
   Readers do not need the original `FrozenLogScope` or source credentials.
2. Call `read_manifest(reference, location, destination_session)`. It reads only
   the exact declared S3 version and checks bytes, size, encryption, scope,
   inventory, chunk limits, sequential ordinals, and exact chunk keys. Missing
   or corrupt declared history raises `ArchiveError`; never convert it to empty.
3. For bounded reads, construct `ArchiveVersionStore(destination_session,
   location)` and use `read_chunk(verified_manifest, index, store)`. The manifest
   must come from step 2. Each chunk read verifies the declared immutable
   version, digest, size, run/operation, order, stream membership, and count.
   `read_events(manifest, location, session)` offers an iterator over all chunks
   and checks the aggregate event digest when exhausted.
4. Task 3 must apply the existing run/task authorization and exact stream mapping,
   literal substring search, timestamp filtering, and cursor semantics. Bind
   cursors to the manifest version/digest and scope. Keep current CloudWatch
   reads after retries; the archive must not hide future log events. Ambiguous
   legacy stream names must not be attributed to a specific task.

Reads cap response bytes before parsing JSON. Default limits are 1 MiB per
chunk, 4 MiB per manifest, 10,000 streams, 10,000 chunks, and 100,000 pages per
source API traversal. Reviewed limits have hard caps of 4 MiB, 16 MiB, 100,000,
100,000, and 1,000,000 respectively. Exceeding a limit leaves the transfer
unverified. One provider page, one event chunk, and bounded stream/chunk metadata
are held in memory; no whole-run event buffer is used. The SDK's CloudWatch
page limits bound individual provider responses.

## Upload failures and recovery

Before any source scan, the journal persists a `scope.json` binding to the
complete scoped-input digest and exact operation prefix. Every resume checks
that binding and every existing journal entry under the exclusive lock. Entries
are bounded by `max_chunks + 4` (chunks, manifest, binding, inventory, and lock);
each JSON record is limited to 16 KiB. A foreign operation, unknown entry,
unbound older journal, or any unresolved upload intent stops the operation
before source scanning or object writes. No older entry is silently adopted.

The first completed source scan also persists `inventory.json`, containing its
counts, digests, group-absence state, and original page counts. Resume compares
stable inventory content; page counts may change. Before publishing a manifest,
the exact set of all recorded chunk versions must match the new scan's chunks.
A smaller or empty scan cannot hide a previous known chunk, unknown accepted
upload, or recorded complete inventory. A saved manifest prevents new object
keys from being added to the same operation.

Each object write persists and fsyncs an intent containing scoped-input digest,
key, content digest, and byte count before `PutObject`. Writes use `AES256`, a
SHA-256 upload checksum, expected bucket owner, and `IfNoneMatch=*`. The exact
returned version ID is persisted and fsynced before readback. Readback requests
that version explicitly, validates its returned version and encryption, and
hashes the bounded bytes. The manifest receives the same checks and is written
last. Journal files contain no event content. Directory entries are fsynced;
an exclusive filesystem lock prevents simultaneous use of the same journal.

A known returned version resumes by verifying the same version without another
upload. A recorded manifest is read and verified by its original exact version,
digest, and bytes. Fresh authority checks and two complete frozen scans still
run. Its stable scope, limits, streams, chunks, and content must match the fresh
result; the original source session ARN and page counts remain historical
observations. Normal session renewal and changed page boundaries do not rewrite
the manifest or create a replacement version. This also permits resume after
the manifest version was recorded but its readback or report delivery failed.

An intent without a version is unresolved, including a timeout after
acceptance, a missing version response, or a crash before version persistence.
The provider does not list/adopt a candidate version, overwrite the key, or
remove any version. A separate reviewed reconciliation must prove ownership or
start a newly approved operation/prefix. Do not erase the intent to retry.
Changed scope, limits, or content conflicts with the whole journal. Preserve
all journal files, including its binding and inventory; do not move individual
object receipts into a new directory to bypass a failed operation.

This is crash safety, not automatic recovery from unknown acceptance. The local
journal filesystem must honor `fsync` and file locks. No cloud or database
execution evidence is supplied by the fake-provider tests.

## Required authority

Source: STS `GetCallerIdentity`, Logs `DescribeLogGroups`, `DescribeLogStreams`,
`FilterLogEvents`, and `Unmask`. Destination: STS `GetCallerIdentity`, S3
`HeadBucket`, `GetBucketVersioning`, `GetBucketTagging`,
`GetBucketOwnershipControls`, exact-version `GetObject`, and prefix-scoped
`PutObject`. Each destination request supplies `ExpectedBucketOwner`.
No delete permission is used by this provider. Role alignment belongs to Task 4.

Same-account relocation must remap and verify every referenced object version
and rewrite the typed reference, or report the archive unresolved. Copying the
manifest bytes alone leaves old version IDs and destination identity in place.
