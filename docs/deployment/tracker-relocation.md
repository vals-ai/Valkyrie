# Same-account terminal run storage relocation

This operator consumes the ValSmith version 1 migration exchange. The request and
response schemas are `storage-migration-request-v1.schema.json` and
`storage-migration-response-v1.schema.json` in this directory. The typed consumer is
`tracker.storage_migration_exchange`. Its generated schemas match the published
producer schemas, including nonempty, unique run and host inventories. There is no
runtime import across repositories.

Run the fixed command through a trusted absolute interpreter and script path:

```text
uv run --project services/tracker python services/tracker/scripts/relocate_run_storage.py \
  --request REQUEST.json --report RESPONSE.json \
  --database-url-env TRACKER_DATABASE_URL \
  --expected-database-target postgresql:localhost:5432/tracker_dev
```

Credentials stay in the named environment variable. The script selects that variable
before tracker engine imports. It compares the request identity with the safe URL
host/port/database and PostgreSQL `current_database()`. A symbolic target such as
`tracker-dev` is not accepted. Reports use atomic replacement, file mode 0600, and
file/directory fsync. Reports contain hashes and control locators, not execution
arguments, artifact contents, secret values, or raw provider errors.

A refusal writes no report. It writes `RESPONSE.json.failure` beside the report with
the nonce, action, run scope, and the authored refusal message, and it prints that
same message on stderr. The failure document is an operator artifact, not part of the
version 1 exchange; the parent reads only the report and only on exit code 0. Durable
per-run progress stays in the run lifecycle checkpoints, so a resume repeats the same
reviewed plan. The report path, and the failure path beside it, may not name the
request or any evidence file.

`inventory` and `inspect` are read-only. `prepare`, `relocate`, and `release` require
`--apply`. The operator does not copy objects, remove source objects, change a bucket
policy, or create an HTTP endpoint. Ordinary retry and resume do not accept a bucket
override. Every response returns the request nonce and action.

## Phases

1. `inventory` reads each exact organization-scoped row and its full stored argument
   digest. It returns nullable legacy labels, real BenchmarkStatus values, saved
   resources, typed reference observations, and an exact completed predecessor.
   Inventory is planning metadata, not a process-drain receipt. It does not fetch or
   materialize tracker result APIs. A held incomplete operation blocks a replacement
   plan.
2. `prepare` consumes the immutable child plan, obtains the operation hold under the
   refreshed run/control locks, commits it, performs strict sandbox cleanup, and
   verifies sandbox absence and positive dispatch drain. A failure leaves the hold.
   The run must be terminal. Active dispatches and pending/building/executing/evaluating
   tasks block preparation; a stop request, expired lease, or FAILED status alone is
   not proof of process exit. No copy may start before preparation succeeds.
3. The parent freezes owner writes and installs its exact operation source fence.
   The fence denies `s3:PutObject` and `s3:DeleteObject`; exact-version cleanup uses
   `s3:DeleteObjectVersion`. The parent copies complete version history, restores an
   existing destination current version when the reviewed collision policy requires
   it, and supplies complete ordered proof.
4. `inspect` rechecks current saved arguments, owned hold, dispatches, tasks, provider
   absence, bucket authority, and supplied object proof. After relocation, omitted
   proof is loaded from the strict durable checkpoint and rechecked against S3.
   A checkpoint never replaces current observations.
5. `relocate` independently reads the current source fence, all source and destination
   versions and markers, exact bytes/hashes/current state, and multipart inventory.
   It rejects missing or extra versions, duplicate identity, unbound transformation,
   and ambiguous history order. It changes only raw `arguments.properties.s3_bucket`
   under the refreshed run lock, commits, reads back, and verifies destination proof
   again. Stored `priority`, `queue_pool_id`, nulls, and all other JSON values survive.
   A commit followed by a failed post-check keeps the hold and resumes through the
   exact checkpoint and child plan.
6. The parent commits its own locations and removes the verified source scope. The
   explicit `release` finalizer requires its matching completion digest, freshly
   verifies source absence, complete destination history, run scope, authority,
   sandbox absence, and dispatch drain. A portable run also needs complete usable
   execution-reference proof. It then releases the hold. An explicit history-only
   run becomes `relocated_history_only` with `released_at` still null. Matching
   finalization can be retried; another completion digest is refused.

Holds remain active across the parent location commit and source cleanup. The shared
PostgreSQL advisory lock namespace prevents purge and relocation operators from
running concurrently on the same run. Provider errors remain errors; cleanup failures
are not converted to success.

## Evidence and retained execution references

Same-account plans can change only the bucket. Region, account, log group, log
retention, organization, owner, labels, full saved execution arguments, and the child
plan digest must match. Both bucket authorities are checked for account and region.
The destination requires managed tags, exact owner identity, and Enabled versioning. The source is the exact
saved bucket and run prefix. A legacy shared source needs no owner tag; any present
organization tag must match. A managed-name source or a source with an owner-account
tag must pass managed-owner validation, with no fallback after a mismatch. Source
versioning is observed as Enabled, Suspended, or never enabled; unknown states fail.
Legacy source `null` VersionIds are accepted only under the fresh operation fence
and exact byte/current-state proof. Destination versions must have non-null IDs.
Cross-account requests fail before mutation and require the separate paired transfer
contract.

A named `stable-host-lifecycle-v1` observation identifies the complete deployed host
inventory. It is external operator evidence, not a claim that this command inspected
host deployment. Each use requires a nonfuture observation at most 15 minutes old and
an acknowledgement cutoff no later than that observation. Legacy started dispatches
need exact `externally_confirmed_host_drain` records and matching evidence file bytes.
The observation must bind the hold acquisition time, dispatch IDs, host inventory,
verifier, and evidence digest. It cannot hide a missing acknowledgement from a current
host. Portable release requires process absence that remains valid without an active
hold; held-unclaimed evidence alone does not permit release.

Every action classifies execution references against the same retired set: every
source bucket named by the operation, not only the bucket of the run being examined.
`inventory` derives that set from the current saved resources of every run in scope
and `prepare`/`relocate`/`release` derive it from the child plan, so a locator into a
bucket the parent will empty cannot be portable at inventory and retired at release.

Saved task results are inspected as well. `EvaluationResult.result` is opaque service
JSON that this tool never rewrites. Any string in it that names a retired bucket is
reported as an unresolved reference, so such a run cannot take a portable policy. A
history-only policy keeps those historical links working only while the source scope
survives; the operator decides that explicitly.

The raw execution digest hashes canonical full stored argument JSON with only
`properties.s3_bucket` removed. Typed references expose a JSON pointer and value hash.
An S3 reference must return an exact version and checksum under the same account;
a reference to the retired source is not portable. This first consumer has no trusted
built-in dataset registry. Null/default/opaque dataset names remain `unknown` and
cannot release a portable run. Use an explicitly reviewed history-only policy or add
verified built-in provenance in a later contract change. The command does not rewrite
execution arguments. It also inspects URI-bearing contract values and non-null Lambda
references; unresolved references remain blocking.

Locator transformations are immutable per-run plan data: exact source bucket/key/
version, source and rewritten sizes/hashes, and ordered JSON pointer edits. The
consumer reads the original bytes and computes the rewrite independently before
location commit. It rejects duplicate JSON keys, invalid UTF-8, nonfinite values,
invalid/colliding pointers, or a changed original value. Encoding uses compact UTF-8
JSON with ASCII escaping and preserved parsed object key order. Each copied or
restored transformed version binds the canonical transformation digest. On later
source cleanup, the already verified immutable proof remains bound by the durable
checkpoint, while destination bytes and remaining source versions are read again.

## Completed history predecessor

`tracker.lifecycle_completion.acquire_successor_hold` is the narrow shared transition.
It requires the exact prior operation UUID, canonical identity/scope digests, and the
canonical complete checkpoint digest for `completed_history_only`. It rechecks the
current run scope and control row under refreshed locks, then replaces the hold in
one transaction. There is no temporary release. Released relocation predecessors
continue through the existing explicit replacement path. Incomplete holds, deletion
tombstones, retired source holds, changed checkpoints, or stale concurrent plans fail.

The existing purge command is not widened by this change. Final production integration
must explicitly wire this transition into later purge/transfer plans and tests.

## Deployment and remaining gates

Deploy the additive lifecycle/checkpoint columns, compatible stable hosts, and tracker
admission guards before this tool. Verify the complete host inventory and legacy drain
scope outside this session. Then review the parent plan, child digest, actual database
target, bucket authority, and explicit run execution policies. This implementation has
local database and provider-boundary tests; it has not contacted deployed services.

This branch predates `Benchmark.log_history`. The consumer refuses a non-null log
history attribute when that production column is integrated. Final production
integration must remap every exact archive VersionId/reference or preserve the
refusal. A bucket-only update cannot preserve version-pinned archive locators.
Cross-account actual runs remain outside this operator.

Inventory reports S3 execution references with a missing or null version as unknown.
This permits an explicit history-only plan for a saved legacy dataset. Portable
release still requires exact immutable retained references. A requested immutable
version that does not match the provider response is rejected.

Object bytes are hashed from a streamed body in bounded chunks, never materialized in
full, so a large version cannot exhaust the operator process. An exact version id is
immutable, so the post-commit re-verification of a `relocate` reuses the digests the
authorizing pass proved in the same process and re-reads only the version listings.
Every standalone action, including `inspect` and `release`, reads the bytes again. The
bounded whole-object read that a planned JSON transformation needs refuses an object
above 64 MiB.

Source and destination histories with equal timestamps for the same key are rejected,
including object/delete-marker ties. Checksums prove bytes, not relative version order.
Inspection with omitted copy/history inputs loads checkpoint proof and hashes the
effective evidence for all runs, in plan run order with each run's proof order retained.
Explicitly supplied proof keeps its original order for response digest compatibility.

The request JSON and external evidence paths are trusted local operator inputs. Keep
them under operator control. This CLI is not a remote request endpoint. Evidence
bytes are read only for a supplied external drain record and its exact digest; they
are never returned in reports. No fixed directory allowlist is imposed.

Portable S3 execution references must pin exactly one nonempty immutable versionId
in the saved locator itself. Discovering a current object's version does not make
an unpinned locator portable. Missing, blank, duplicate, or null versions remain
unknown without fetching the current object. History-only plans retain these stored
arguments and keep their admission hold.
