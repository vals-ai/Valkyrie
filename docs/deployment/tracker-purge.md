# Tracker run deletion

This private operator command removes one exact tracker slice. It does not prove
that the parent owner deletion is complete. It does not delete organizations,
executor releases, admission controls, another run's data, or shared log groups.

Use `services/tracker/scripts/purge_run_data.py` from this repository with `uv`.
The default action is the read-only `plan`. Each mutation requires `--apply`.
There is no public API, retry override, bucket override, or hold-release option.

## Deployment and evidence gate

Deploy additive migration `8c9d0e1f2a3b` after `7b8c9d0e1f2a`. Deploy hosts that
obey the shared lifecycle hold and write positive exit acknowledgements. Verify
the complete deployed host inventory before using prepare or purge. Read
[the shared lifecycle contract](tracker-lifecycle.md) for the host rollout and
external legacy drain procedure. These changes do not perform that rollout.

`--host-contract` is a current operator observation under the shared
`HostContractObservation` schema. It is an external deployment observation, not a
claim that this CLI queried ECS. Inspect the complete deployment on every apply
or resume. Unknown compatibility leaves the operation pending. The command
reads fresh dispatch rows and accepts only the foundation's positive exit,
verified normal-finish, or held-unclaimed evidence. A force-stop status, expired
lease, or old heartbeat does not prove exit.

For legacy dispatches, repeat `--external-host-drain receipt.json` and
`--external-evidence evidence-file` as needed. Each strict external receipt binds
the operation, exact run/dispatch list, hold acquisition time, host inventory,
verifier, observation time and SHA-256 of the supplied evidence file. These
confirmations remain labelled `externally_confirmed_host_drain`. The same receipt
and evidence bytes are required on resume after row deletion. The command does
not stop the host inventory or run the external legacy gate.

## Input and output contracts

[tracker-purge.schema.json](tracker-purge.schema.json) contains `PurgePlan`,
`PurgeReport`, `FenceReceipts` (an array), the exact `OwnerDeletionFence` statement,
and the private `PurgeCheckpoint`.
These reuse the shared identity, scope and drain models in
[tracker-lifecycle.schema.json](tracker-lifecycle.schema.json).

A plan has `identity` and sorted `runs`. Each run has a `scope` with complete
original saved resources and exact object/log paths, plus a `provider` containing
only `kind` and `secret_name`. A run that follows a completed relocation also
has `released_relocation`: the exact prior operation UUID, SHA-256 hashes of its
stored identity and scope JSON, and its UTC acquisition/release times. The
read-only plan captures this from the retained released relocation record. It
refuses active relocation and permanent deletion records. The current saved
resources in `scope` describe the destination bucket after relocation; the
predecessor scope hash binds the previous original-resource record.

Create a new deletion operation and plan only after relocation has completed and
its hold is released. Prepare locks and refreshes the run and control record,
compares every planned predecessor fact, then uses the shared explicit
`replace_released_operation_id` path. An absent, changed, active, permanent, or
unplanned predecessor is refused before provider deletion. Each run has its own
predecessor. The tracker does not automatically replace another operation.
A matching deletion resume uses the original plan and its durable checkpoint;
it does not need the previous relocation record to remain after replacement.

The secret locator is private control data; no secret values or benchmark arguments are saved in the plan/checkpoint. A report
contains the shared lifecycle fields, `child_plan_sha256`, and `outcome`:
`checked` means this invocation completed its requested checks; `incomplete`
means current verification failed. A `prepared` phase authorizes only the next
parent handshake. A saved `complete` phase in an incomplete report is not fresh
proof. The exit code is 0 only after the requested checks pass and the report is
written; failures return 2. Error output omits provider and SQL payloads.

The child digest is SHA-256 of canonical plan JSON with sorted keys, compact
separators and absent optional values omitted. In particular, an absent
`released_relocation` is omitted, so existing plans without this field keep
their original digest and can resume. Plan output uses the same omission rule.
The child digest is distinct from the immutable parent plan fingerprint. Both stay
bound to the durable hold. Changed scope, operation, owner, org, account,
resources, provider locator or database target is refused.

Use a named environment variable for the database URL, for example
`TRACKER_PURGE_DATABASE_URL`. Credentials never appear in arguments or reports.
The bootstrap reads this variable before tracker imports. `database_target` and
`--expected-database-target` must both equal
`postgresql:<host>:<port>/<database>`, derived from the connection URL and checked
with `SELECT current_database()`. A Unix-socket directory replaces `<host>` when
present in the URL query. The command requires the public PostgreSQL schema.
Plan/report writes are atomic and mode 0600. Keep all evidence in a private
operator directory. Plan and report paths must be distinct.

## Handshake

From the repository root, using private JSON files and an already set database
environment variable:

```sh
uv run --project services/tracker python services/tracker/scripts/purge_run_data.py plan \
  --database-url-env TRACKER_PURGE_DATABASE_URL \
  --expected-database-target 'postgresql:tracker.internal:5432/tracker' \
  --identity /private/identity.json --plan /private/purge-plan.json

uv run --project services/tracker python services/tracker/scripts/purge_run_data.py prepare --apply \
  --database-url-env TRACKER_PURGE_DATABASE_URL \
  --expected-database-target 'postgresql:tracker.internal:5432/tracker' \
  --plan /private/purge-plan.json --report /private/prepare-report.json \
  --host-contract /private/current-host-contract.json
```

Prepare validates saved scope and bucket authority, acquires the permanent hold,
captures original dispatch facts, applies full force-stop, deletes exact run
sandboxes through the strict provider API, verifies process drain, and repeats
sandbox inventory. It removes no objects, logs, or database rows. Unexpected
provider errors propagate; only the provider's explicit not-found error is
accepted before the second inventory.

After all tracker preparations and local writer shutdown, the parent installs
and verifies the owner-wide write fence. The tracker never installs it or sends
the write probe. The exact statement is:

```json
{
  "Sid": "ValSmithOwnerDeletion<operation UUID without hyphens>",
  "Effect": "Deny",
  "Principal": "*",
  "Action": ["s3:PutObject", "s3:DeleteObject"],
  "Resource": "arn:aws:s3:::<exact validated owner bucket>/*"
}
```

The action array must use this exact order. The old PutObject-only fence is
refused: an unversioned DeleteObject request can create a late delete marker.
DeleteObjectVersion remains allowed for exact-version cleanup, including a
probe version returned by an unexpected successful write. No condition or extra
statement fields are accepted for that SID. The schema fixes the action shape;
runtime checks also bind Sid and Resource to the current operation and bucket. The parent
preserves unrelated policy statements. Each fence receipt contains the full
operation `identity`, exact `bucket`, SHA-256 `policy_sha256` of canonical full
policy JSON, UTC-aware `observed_at`, and:

```json
{
  "write_probe": {
    "key": ".valsmith-owner-deletion/<canonical hyphenated operation UUID>/write-probe",
    "outcome": "AccessDenied"
  }
}
```

The parent must resolve any unknown probe acceptance before retrying. An
unexpected successful probe requires cleanup of its exact returned VersionId;
it cannot produce an accepted fence receipt. The tracker re-reads current bucket
policy with `ExpectedBucketOwner`, compares the full digest and exact deny
statement, and rejects absent, altered, future-dated or wrong-operation proof.
It separately checks the live STS account, bucket account/region, managed name,
org/environment/backup tags, enabled versioning, and exact parent GitHub owner tag.

```sh
uv run --project services/tracker python services/tracker/scripts/purge_run_data.py purge --apply \
  --database-url-env TRACKER_PURGE_DATABASE_URL \
  --expected-database-target 'postgresql:tracker.internal:5432/tracker' \
  --plan /private/purge-plan.json --report /private/purge-report.json \
  --host-contract /private/current-host-contract.json \
  --fence-receipts /private/owner-fence-receipts.json
```

Use `resume` with the same arguments and immutable plan to retry an incomplete
purge. The command always checks current provider absence and fence authority.
An absent run without a strict matching checkpoint never counts as success.
The shared host observation freshness checks apply on every resume, including
a run whose Benchmark row is already absent. Evidence must use UTC, cannot be
future-dated, and cannot be more than 15 minutes old at use.

## Phases and retained state

`held -> prepared -> objects_removed -> logs_removed -> rows_removed -> complete`

One session-level PostgreSQL advisory lock per run prevents concurrent purge
callers across provider calls and transaction commits. The existing run lock
then the lifecycle record lock protect each checkpoint update. No second fence
table is created. The nullable text checkpoint retains only the plan digest,
provider locator, exact planned relocation predecessor, original dispatch facts,
drain proof, scoped row IDs, fence
policy digest and phase. The deletion hold and checkpoint survive row removal.

Purge lists all versions and delete markers under the saved
`benchmarks/<run UUID>/` prefix, deletes each exact Key/VersionId, lists and
aborts each exact multipart upload, then inventories again. This also removes
any archived run-log payloads beneath that prefix. It deletes only the exact
saved `<log-group>/<run UUID>` group and inventories again. Pagination or API
errors leave cleanup incomplete.

Before provider removal and again inside the SQL transaction, purge inspects
PostgreSQL foreign keys. Unknown child edges, missing expected edges, cross-schema
edges, and composite edges stop deletion. It deletes evaluation/error results,
final evaluation, dispatches, tasks, the run, and only unreferenced task breakdowns.
Shared task breakdowns remain. The row transaction briefly locks these tables to
prevent foreign-key schema changes and concurrent child inserts during deletion.
A SQL error rolls back row deletion but retains previous provider phases for
resume. The row-removal checkpoint commits atomically with those deletes. Fresh
policy, sandbox, storage and row post-checks run after that checkpoint and
precede `complete`. Host observation freshness is checked again after the final
provider calls and before a checked report. A late marker or failed final check
leaves the row-removal checkpoint available for the same-plan resume.

Database and provider changes are not one distributed transaction. A failure can
leave already removed provider data with retained rows. This is deliberate:
permanent holds, immutable scope and checkpoints let the same operation resume.
The parent owner freeze and write fence must remain until all parent checks pass.
