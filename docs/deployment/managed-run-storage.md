# Managed run storage deployment

Managed run storage lets an authorized ValSmith organization select an application-provisioned S3 bucket for a managed Valkyrie run. Valkyrie validates and saves the selected location. The saved location remains authoritative for execution, results, reads, retry, and recovery.

## Deployment configuration

Set these values explicitly for each Valkyrie deployment:

- `AWS_DEPLOYMENT_ROLE_ORG_IDS`: comma-separated canonical organization UUIDs that can use managed AWS execution.
- `AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS`: a JSON object from an organization UUID in `AWS_DEPLOYMENT_ROLE_ORG_IDS` to a non-empty list containing `dev`, `prod`, or both. An absent value is `{}` and authorizes no owner bucket.
- `AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED`: `true` enables new owner-storage submissions. The default is `false`.
- `AWS_MANAGED_STORAGE_VALIDATION_TTL_SECONDS`: how long one Tracker or ExecutorHost process may reuse a successful owner-bucket validation for a read. The default is `300`. `0` revalidates on every read, which multiplies throttle-prone bucket-level S3 calls. Admission still validates on every submission, and a refusal is never remembered.

The submission flag controls only new owner-storage admission. Saved owner runs still use their saved locations for reads, execution, retry, and recovery while their organization/environment authorization remains configured.

The deploy workflow takes both values from the GitHub Environment for the target stage: `dev` for dev, `prod` for bench, and `prod-external` for production. They are defined under the same names as the settings:

- `AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS` is an Environment **secret**, like `AWS_DEPLOYMENT_ROLE_ORG_IDS`, because it carries the same organization UUIDs. An absent secret synthesizes `{}`.
- `AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED` is an Environment **variable**. An absent variable synthesizes `false`.

Every deployment job that synthesizes CDK passes both. Define them in the GitHub Environment, not only in a manual `make deploy`: a later ordinary deploy re-synthesizes from the Environment and would otherwise reset the map to `{}` and remove owner-bucket IAM from both task roles. Synthesis fails if `AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED` is `true` while `AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS` is empty.

CDK supplies `AWS_DEPLOYMENT_ACCOUNT_ID` from the stack account. It passes the same account, canonical organization/environment JSON, and submission flag to Tracker and ExecutorHost.

The bench AWS account is `613431292675`. ValSmith production currently uses the bench Valkyrie deployment. A local AWS profile named `vals-prod` is not evidence of access to the external production account. Obtain and verify the external production account value through its deployment owner before a production deployment.

The configuration does not derive storage environments from the Valkyrie stage. In particular, bench does not imply `dev`, `prod`, or a `vs-bench-*` bucket pattern. Each organization/environment pair must be present in `AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS`.

## IAM boundary

Tracker and ExecutorHost receive owner-bucket grants only for the configured environment union. Each Allow requires `s3:ResourceAccount` to equal the stack account. A Deny rejects a different resource account and is limited to the same `vs-dev-*` and `vs-prod-*` bucket and `benchmarks/*` object patterns.

Both roles can list a configured owner bucket, inspect its tags and versioning, and read and write `benchmarks/*`. Tracker can delete objects and exact object versions for rollback. ExecutorHost can abort multipart uploads. Owner-bucket grants do not include `agents/*`, bucket creation, tag writes, bucket-policy writes, or other bucket administration. Agent-library reads continue to use the shared Valkyrie bucket. The ExecutorHost release bucket remains a separate policy boundary.

The ValSmith provisioner must create and tag owner buckets before use. The bucket tags must include the matching `valsmith:environment`, `valsmith:owner-account-id`, `valsmith:backup=true`, and `valsmith:valkyrie-org-id`. Versioning must be enabled.

## Separate prerequisites and owners

Complete these changes through their own repositories and deployment owners before enabling owner storage:

- The benchmarks service registry owner must add the required ValSmith generation and dataset service-role reads, lists, and established writes. Keep legacy reads during migration.
- The owners of output and analyzer Lambda execution roles must add the required owner-bucket access. Valkyrie's Lambda invocation permission does not grant the Lambda role S3 access.
- The external OIDC role owners must update their policies for the configured owner-bucket patterns.
- The ValSmith owner must deploy bucket provisioning and tags, per-run source-bucket persistence, and per-run result-publication source selection.

These are separate delivery slices. Valkyrie deployment does not create or update them.

## Rollout order

Use this order for each environment:

1. Confirm the explicit Tracker URL, stack AWS account and region, organization/environment map, ValSmith expected account, and shared bucket. Keep owner-storage writes disabled in ValSmith and Valkyrie.
2. Apply Valkyrie infrastructure IAM and registry IAM through their separate approved deployments. Update the external Lambda and OIDC policies. Do not enable the application before these policies exist.
3. Deploy ExecutorHost processes that understand protocols 1, 2, and 3. Fully replace hosts that support only protocols 1 and 2 before Valkyrie admits a protocol 3 dispatch. Build and register the new immutable protocol 3 executor artifact, but do not activate it under old Tracker processes.
4. Pause managed submissions for the short cutover. Drain or replace old Tracker processes, deploy Tracker with protocol 3 admission and the storage API, and activate the protocol 3 release. The old Tracker requires protocol 2, so activating protocol 3 while old producers remain causes failed starts. Re-enable ordinary managed submissions only after every producer and host is compatible.
5. Keep protocol 2 artifacts for already admitted protocol 2 dispatches. Protocol 3 hosts can run them. An in-progress protocol 2 run stays pinned to its release. New retry or resume admission for it reports a compatibility error. Let it finish or use the existing controlled stop and terminal recovery path to select protocol 3. Do not change an active release or its protocol metadata.

   Before you activate the protocol 3 release, pause the ValSmith reconciler, or confirm that it treats this refusal as transient. Valkyrie refuses `POST /retry-or-resume-benchmark/{id}` for every **managed** run that is still `IN_PROGRESS` on a pinned protocol 2 release. The response is **HTTP 409** with the detail `Activate an executor release that supports managed runs`; the same refusal on a terminal run is HTTP 503, but a terminal run selects the active protocol 3 release and therefore succeeds. The refusal changes no state: no dispatch row, no status change, no queue message. A reconciler that records it as a run failure marks healthy runs failed for the whole cutover window. Restart reconciliation after every in-progress managed run has finished or been stopped and recovered onto protocol 3. Access-key runs and unmanaged runs are unaffected.
6. Install the new SDK in ValSmith. Deploy per-run source-bucket support and the provisioner organization tags. Then enable owner-storage submissions. The new endpoint fails closed against an old Tracker. Test a non-production owner run, frozen bundle, terminal artifacts, result publication, and recovery before enabling production traffic.

## Rollback order

1. Disable new owner-storage submissions in Valkyrie.
2. Disable ValSmith owner-write entry points.
3. Leave protocol 3 readers, hosts, artifacts, IAM, and saved locations in service for admitted owner runs.

Do not roll back to production code that cannot parse `BenchmarkArguments.properties` while owner-storage rows exist. Rollback does not rewrite saved bucket fields or move objects. Promotion to production must include the runtime-persistence changes already required from dev and must preserve the unique production CLI environment selection. Do not apply this branch as an isolated change to old production code.

## ValSmith integration contract

`StageRun.storage_bucket` and `ModelRun.storage_bucket` must store the actual bucket returned by Valkyrie for each run. They are run outputs, not values copied from the owner's current bucket. After a lost start response, recover the location with the organization-scoped run detail or metadata endpoint.

`runs.start` reports the two owner-storage failures as two exception types, and callers must branch on the type rather than on `run_id` or on exception text:

- `ValkyrieRunAcceptedError`, a subclass of `ValkyrieRunError`, means Valkyrie created the run in the requested bucket but did not acknowledge the executor dispatch. The storage is correct. Reconcile or retry by `run_id`; do not record a storage rejection. The original `ValkyrieAPIError` is the cause.
- Plain `ValkyrieRunError` with a `run_id` means the start response carried a missing or different `storage_bucket`. That is a storage rejection. Do not submit again without an explicit override.

A caller that catches only `ValkyrieRunError` still sees both, so an old handler keeps working, but it cannot tell them apart and will quarantine correctly stored runs.

`DatasetViewRun.source_bucket` is selected per run. A view may read old runs from shared storage and new runs from different owner buckets. `DatasetViewRequest.destination_bucket` remains the dataset bucket. A null legacy per-run column uses ValSmith's documented legacy location. Publication must not replace a saved source with an owner's current bucket.

The provisioner must set `valsmith:valkyrie-org-id` to the canonical Valkyrie organization UUID before admission. ValSmith owns its two per-run columns, database migrations, provisioning, SDK pin, publication, and Lambda source selection. This repository makes none of those changes.

Use a reviewed immutable SDK commit that includes `ValkyrieRunAcceptedError`, the optional structured `ValkyrieRunError.run_id`, and the `SingleBenchmarkResponse.storage_bucket` field. The earlier SDK pin without those fields is insufficient for reconciliation. Registry IAM and external Lambda/OIDC policy deployments must precede owner writes; ValSmith application rollout must follow the compatible tracker, host, and executor-release cutover.

## Verification and remaining release gates

Local tests cover saved locations with fake AWS operations and disposable PostgreSQL databases. They cannot prove deployed IAM or account configuration. Before production writes, complete the non-production smoke run through bundle freeze, terminal results, publication, download, and recovery. Record the tracker and host revisions, immutable executor artifact and protocol, SDK commit, and deployed IAM/account settings. Cloud deployments, service-role changes, and production enablement remain separate operator release gates.
