# Read-only purge inspection v1

Run the trusted Tracker interpreter and script with no shell. Credentials stay
in the named database environment variable and the existing scoped AWS boundary.

```text
<interpreter> <checkout>/services/tracker/scripts/purge_run_data.py inspect --database-url-env NAME --expected-database-target TARGET --plan PLAN.json --report REPORT.json --request-nonce UUID [--host-contract HOST.json] [--fence-receipts FENCES.json] [--external-host-drain DRAIN.json --external-evidence FILE]
```

`inspect --apply` is rejected before opening the database. Successful inspection
writes a private atomic report and exits 0. Any unknown scope, provider failure,
missing proof or output failure exits 2. A saved report is usable only with the
matching fresh request nonce and exit 0. Failure does not publish partial proof.

The exact machine-readable files are:

- `tracker-purge-inspection-v1.schema.json`: response schema.
- `tracker-purge-plan-v1.schema.json`: immutable input plan schema.
- `fixtures/tracker-purge-inspection-v1.json`: valid mixed response.
- `fixtures/tracker-purge-inspection-plan-v1.json`: matching input plan.

The top-level `identity` binds the exact operation, parent fingerprint, owner,
org, account pair, database, region, environment and sorted run UUIDs.
`child_plan_sha256` hashes canonical immutable plan JSON, with null fields
excluded. `request_nonce` echoes this invocation's UUID. `observed_at` is UTC.
There is one observation per planned run in the same order.

Each observation has exact `scope`, `provider` and nullable
`expected_run_label`. The non-null expected label is an immutable PurgeRun field.
Existing plans that omit it keep their original digest. Null or omission grants
no label authority, including after row removal. For present rows, non-null
expected labels must match the fresh locked current label. Prepare and purge
repeat that check under fresh mutation locks. Inspection does not replace those
checks. Set expected labels in the reviewed plan before the first prepare; a
changed plan cannot adopt an existing checkpoint from another child digest.

- `present_unheld`: row exists, no active hold exists, and any released relocation
  equals the planned predecessor. `current_label` is fresh and nullable.
  `released_relocation` is the exact predecessor or null. No process/provider
  absence is asserted; preparation can still be needed.
- `present_held`: row exists under the exact active deletion hold and immutable
  checkpoint. `current_label` is fresh and nullable. `checkpoint` supplies phase,
  SHA-256 of the exact stored checkpoint JSON bytes, and child plan digest.
  A `held` phase can represent failed preparation; it asserts no absence.
  Later phases also require current process drain and provider sandbox absence.
- `removed`: no row exists, the exact active deletion hold and checkpoint survive,
  and phase is `rows_removed` or `complete`. It contains no current label.
  `expected_run_label` is historical authority only through the exact immutable
  child digest; a caller claim alone is never evidence. Current checks prove the
  scoped rows, sandboxes, objects, delete markers, multipart uploads and exact log
  group absent. `fence_policy_sha256` is freshly verified and must equal the saved
  checkpoint fence. `absence` is the literal `rows_sandboxes_objects_logs`.

All observations verify current account/region/managed owner bucket identity.
Held phases after `held` and removed observations require a fresh host contract.
Removed observations also require the exact current fence receipt and any saved
external drain receipt plus its exact evidence bytes. Host freshness is rechecked
before return. These checks use the existing provider boundary; secret values
remain in memory and never enter this response.

The command takes advisory and row locks for consistent observations. It never
acquires or replaces a lifecycle hold, stops a run, deletes provider data or
rows, updates a checkpoint, flushes pending writes, or commits a transaction.
It returns mixed present/removed progress explicitly. Unknown missing runs and
unrelated holds are errors. No destructive resume is needed to inspect state.
