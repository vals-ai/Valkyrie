# Tracker lifecycle contract

This is the shared foundation for later relocation and deletion operators. It does
not expose an HTTP API or perform purge, copy, provider cleanup, or relocation.
Confidence: high for the local contract. Deployed host compatibility is unknown.

## Deployment gate

1. Apply additive migration `7b8c9d0e1f2a`, after `6a7b8c9d0e1f`.
2. Deploy compatible stable hosts. Every host must obey active `runlifecycle`
   holds on claim, current-authority, heartbeat and finish. Hosts must write
   `executordispatch.process_exited_at` after they await actual child exit.
3. Deploy tracker guards, then the later operator tools.
4. Keep apply disabled until a fresh inspection identifies the complete deployed
   host inventory, its contract and the legacy dispatch inventory. Unknown
   compatibility leaves the operation pending.

The schema is additive and its downgrade is refused. Do not backfill exit receipts
from status, `finished_at`, heartbeat, lease age, or stop responses. Dispatch UUIDs
are single-use: only QUEUED dispatches can be claimed, and retries create new IDs.
An exit acknowledgement matches dispatch and run UUIDs, sets database current time
only once, and changes no status or resource. It remains available after FAILED
and during a hold. A failed acknowledgement leaves drain pending.

## Identity and hold interface

`tracker.lifecycle` owns `OperationIdentity`, `RunScope`, `acquire_hold`,
`require_owned_hold`, `require_unheld`, and `release_relocation_hold`. Transactions
belong to callers. Callers must acquire executor admission before the run lock
when they need both. Acquisition locks the run, checks exact saved resources and
org, and records a minimal durable identity without a Benchmark or Org foreign key.
It never stores arguments, contracts, results, credentials, or provider errors.

`OperationIdentity` fields are exactly:

| Field | Type / rule |
| --- | --- |
| `schema_version` | integer `1` |
| `operation_id` | canonical UUID |
| `parent_plan_sha256` | 64 lowercase hexadecimal characters |
| `github_owner_id` | positive integer; booleans are rejected |
| `org_id` | canonical UUID |
| `source_aws_account_id`, `destination_aws_account_id` | 12 digits |
| `region`, `environment`, `database_target` | nonempty safe identifiers |
| `run_ids` | nonempty sorted unique UUID array |

`database_target` is an approved non-secret database label, never a connection URL.
The parent computes the plan digest from canonical immutable plan data, excluding
observation timestamps and new receipts. Tools compare all identity fields on
resume. A changed owner, org, account, database, resource location or run scope
needs a replacement plan. A matching record can prove the original scope after
run removal; absent data alone is not success.

`RunScope` contains `run_id`, `original_resources`, `object_prefix`, and `log_group`.
Resources have exactly `region`, `s3_bucket`, `log_group`, and positive
`log_retention_days`. The last two scope fields must equal `benchmarks/<run UUID>/`
and `<original_resources.log_group>/<run UUID>`. The constructor derives missing
location fields. Resume compares their canonical full values.

A deletion hold is permanent, including after run deletion. Re-acquisition by the
same exact operation does not release a hold. A relocation hold is released only
through `release_relocation_hold(identity=..., scope=..., verify_completion=...)`.
The in-process callback receives `(session, record)` under the run lock. It must
freshly read and verify source cleanup, destination copies/current object state,
tracker resources, the ValSmith location transaction and all required final phases.
It must raise for missing, stale or incomplete facts. It must not accept a saved
success flag as proof. Exceptions retain the hold. The caller commits successful
release explicitly; it keeps the released record. A new operation must provide
`replace_released_operation_id` to replace that exact released relocation record.
This cannot replace a deletion hold. Acquisition rejects an already released operation;
use `require_owned_hold` to read its audit record. Ordinary retry cannot invoke this release.

## Drain evidence

`tracker.lifecycle_evidence.verify_drain` takes the same `identity`, `scope` and
`purpose`, plus a fresh `HostContractObservation` or `None`. It reads the durable
active hold and current dispatch rows. The returned tuple has one `DispatchDrain`
per dispatch, sorted by UUID. Each contains `dispatch_id`, `provenance`, and nullable
`observed_exit_at`. Provenance is one of:

- `host_process_exit`: positive database receipt from the actual child-exit boundary.
- `verified_finished_contract`: FINISHED from the explicitly inspected host contract.
- `held_unclaimed`: captured unstarted state, active hold and compatible host contract.
- `externally_confirmed_host_drain`: the separate legacy operator gate below.
- `pending`: positive evidence is missing. Apply must stop.

FAILED, `finished_at`, elapsed time, a stop request, and expired leases do not prove
exit. No host observation means FINISHED and unclaimed claims remain pending.
A timestamp receipt alone never proves provider sandbox absence or object cleanup.

`HostContractObservation` is built by a fresh deployment inspector in the operator,
not loaded as trusted input from a parent report. Its fields are `contract` (literal
`stable-host-lifecycle-v1`), `deployment_sha256`, sorted unique nonempty
`host_inventory`, timezone-aware `observed_at`, timezone-aware
`acknowledgement_required_since`, named `verifier`, and `legacy_dispatch_ids`.
The cutoff and legacy IDs come from the inspected rollout/inventory. They must not
be chosen to relabel a current host acknowledgement failure as legacy.

### Separate legacy host-drain gate

Historic started dispatches without receipts stay pending. An operator must:

1. Identify the complete relevant host inventory and the deployed host contract.
2. Prevent old hosts from taking new work, including replacement hosts.
3. Stop every relevant host and verify actual termination of each host.
4. Record the exact pending legacy dispatch list, the held operation and run,
   acquisition time, host inventory, observation time and verifier identity.
5. Hash the evidence file and provide its bytes for digest validation.

`ExternalHostDrain` fields are exactly `provenance` (literal
`externally_confirmed_host_drain`), full `identity`, `run_id`, timezone-aware
`hold_acquired_at`, sorted exact `dispatch_ids`, exact `host_inventory`,
`deployed_host_contract`, timezone-aware `observed_at`, named `verifier`,
`evidence_sha256`, and `confirmation` (literal
`all_inventory_hosts_terminated_and_old_claims_disabled`). The verifier matches the
full pending legacy set, active hold, supplied file digest and inspected inventory.
It refuses current-host dispatches at or after the acknowledgement cutoff. It
retains distinct external provenance and no invented process-exit timestamp.
This is external testimony, not a tool-verified process exit. It cannot hide
provider cleanup errors. A bare flag, stop-request count or maintenance command
is insufficient. The existing maintenance command does not wait for every host
to terminate and must not serve as this gate. This session did not execute the gate.

## Shared report JSON

`LifecycleReport` fields are `identity`, `purpose` (`relocation` or `deletion`),
timezone-aware `observed_at`, nullable `host_contract`, and `runs`. `RunReport`
fields are `scope`, `phase`, nullable `destination_resources`, `copied_objects`,
`dispatch_drain`, and nullable `external_host_drain`. Copy records have `key`,
`source_version_id`, `destination_version_id`, nullable `checksum_sha256`, and
`state` (`version`, `current_object`, `current_delete_marker`, or `delete_marker`).
Relocation operators must require complete destination version/checksum/current
state evidence before relocation. No copy record or phase name authorizes action.

The companion [JSON schema](tracker-lifecycle.schema.json) is the exact report
shape. [Example JSON](tracker-lifecycle-example.json) shows a held deletion plan
receipt with no drain claim. These files are portable; consumers need no runtime
imports from this repository. Unknown fields are rejected. `write_report` verifies
the full sorted run scope, writes a mode-0600 temporary file, flushes it, and
atomically replaces the report.

The parent must bind the report to its exact plan and perform fresh read-only
tracker, provider and owner-graph checks before its final commit. Reports are
observations, not permission tokens. Deletion must still follow prepare and drain,
owner write fence, then rechecked purge. Provider absence must be inventoried again
after drain to catch sandbox creation already in flight. All apply paths must
retain holds and the owner freeze on failure.
