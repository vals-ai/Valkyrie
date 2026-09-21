# Managed run storage deployment

Managed run storage lets an authorized ValSmith organization select an application-provisioned S3 bucket for a managed Valkyrie run. Valkyrie validates and saves the selected location. The saved location remains authoritative for execution, results, reads, retry, and recovery.

## New-run scope

This change routes new runs. Existing runs retain their saved bucket and log location. No data migration, historical log export, deletion operator, or database schema migration is required by this PR.

An owner-storage start saves the log prefix `<AWS_DEPLOYMENT_LOG_GROUP>/<managed_s3_bucket>`. CloudWatch groups are created at `<prefix>/<run-id>`. For example, a deployment prefix of `/valkyrie/benchmarks` and bucket `vs-prod-acme-123` produce `/valkyrie/benchmarks/vs-prod-acme-123/<run-id>`. The caller selects the authorized bucket; the server derives the log prefix within the existing deployment IAM boundary. Region and log retention remain deployment settings. Execution, log reads, and recovery use the saved prefix even after deployment defaults change. Ordinary starts without an owner bucket keep the existing log layout.

Tracker and executor processes use their managed AWS roles. ValSmith does not send AWS session credentials, and execution retains the AWS SDK's credential renewal. Agent bundles remain published once in the shared library. Admission copies the selected bundle directly from `shared-bucket/agents/<agent>.zip` to `owner-bucket/benchmarks/<run-id>/<agent>.zip`; it does not require an `agents/` library in each owner bucket. Recovery reuses the frozen run copy. The CLI `--update-agent` option supports updates within its configured bucket only. It rejects runs in a different bucket before writing or resuming; omit the option for owner runs.

## Deployment configuration

Set these values explicitly for each Valkyrie deployment:

- `AWS_DEPLOYMENT_ROLE_ORG_IDS`: comma-separated canonical organization UUIDs that can use managed AWS execution.
- `AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS`: a JSON object from an organization UUID in `AWS_DEPLOYMENT_ROLE_ORG_IDS` to a non-empty list containing `dev`, `prod`, or both. An absent value is `{}` and authorizes no owner bucket.
- `AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED`: `true` enables new owner-storage submissions. The default is `false`.
- `AWS_MANAGED_STORAGE_VALIDATION_TTL_SECONDS`: how long one Tracker or ExecutorHost process may reuse a successful owner-bucket validation for a read. The default is `300`. `0` revalidates on every read, which multiplies throttle-prone bucket-level S3 calls. Admission still validates on every submission, and a refusal is never remembered.

The submission flag controls only new owner-storage admission. Saved owner runs still use their saved locations for reads, execution, retry, and recovery while their organization/environment authorization remains configured.

The deploy workflow takes both values from the GitHub Environment for the target stage: `dev` for dev, `prod` for bench, and `prod-external` for production. They are defined under the same names as the settings:

- `AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS` is an Environment **secret**, like `AWS_DEPLOYMENT_ROLE_ORG_IDS`, because it carries the same organization UUIDs. An absent secret synthesizes `{}`. Do not move it to an Actions variable: every job reads `secrets.AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS`, so a map stored as a variable resolves to the `{}` default and the next deploy removes every owner-bucket grant without reporting an error.
- `AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED` is an Environment **variable**. An absent variable synthesizes `false`.

Organization UUIDs are identifiers, not credentials. CDK places these settings in ECS task definitions. Access requires a valid organization API key and a bucket in the deployment account with matching tags.

Every deployment job that synthesizes CDK passes both. Define them in the GitHub Environment, not only in a manual `make deploy`: a later ordinary deploy re-synthesizes from the Environment and would otherwise reset the map to `{}` and remove owner-bucket IAM from both task roles. Synthesis fails if `AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED` is `true` while `AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS` is empty.

CDK supplies `AWS_DEPLOYMENT_ACCOUNT_ID` from the stack account. It passes the same account, canonical organization/environment JSON, and submission flag to Tracker and ExecutorHost. Local CLI AWS profiles use their own SDK credential chain and do not require this deployment setting.

The configuration does not derive storage environments from the Valkyrie stage. In particular, bench does not imply `dev`, `prod`, or a `vs-bench-*` bucket pattern. Each organization/environment pair must be present in `AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS`.

## IAM boundary

Tracker and ExecutorHost receive owner-bucket grants only for the configured environment union. Each Allow requires `s3:ResourceAccount` to equal the stack account. A Deny rejects a different resource account and is limited to the same `vs-dev-*` and `vs-prod-*` bucket and `benchmarks/*` object patterns.

Both roles can list a configured owner bucket, inspect its tags and versioning, and read and write `benchmarks/*`. Tracker can delete objects and exact object versions for rollback. ExecutorHost can abort multipart uploads. Owner-bucket grants do not include `agents/*`, bucket creation, tag writes, bucket-policy writes, or other bucket administration. Agent-library reads continue to use the shared Valkyrie bucket. The ExecutorHost release bucket remains a separate policy boundary.

The ValSmith provisioner must create and tag owner buckets before use. The bucket tags must include the matching `valsmith:environment`, `valsmith:owner-account-id`, `valsmith:backup=true`, and `valsmith:valkyrie-org-id`. Versioning must be enabled.

## Separate prerequisites and owners

Complete these changes through their own repositories and deployment owners before enabling owner storage:

- The benchmarks service registry owner must add the required ValSmith generation and dataset service-role reads, lists, and established writes. Keep access to existing runs in their original locations.
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

   During cutover, pause the ValSmith reconciler or make it treat HTTP 409 with `Activate an executor release that supports managed runs` as transient. This response leaves the run and queue unchanged. Restart reconciliation after in-progress managed runs finish or recover onto protocol 3.
6. Install the new SDK in ValSmith. Deploy per-run source-bucket support and the provisioner organization tags. Then enable owner-storage submissions. The new endpoint fails closed against an old Tracker. Test a non-production owner run, frozen bundle, terminal artifacts, result publication, and recovery before enabling production traffic.

## Rollback order

1. Disable new owner-storage submissions in Valkyrie.
2. Disable ValSmith owner-write entry points.
3. Leave protocol 3 readers, hosts, artifacts, IAM, and saved locations in service for admitted owner runs.

Do not roll back to production code that cannot parse `BenchmarkArguments.properties` while owner-storage rows exist. Rollback does not rewrite saved bucket fields or move objects.

## ValSmith integration contract

`StageRun.storage_bucket` and `ModelRun.storage_bucket` must store the actual bucket returned by Valkyrie for each run. After a lost start response, recover the location with the organization-scoped run detail or metadata endpoint.

`runs.start` reports two outcomes after an owner run has been created:

- `ValkyrieRunAcceptedError`, a subclass of `ValkyrieRunError`, means Valkyrie created the run in the requested bucket but did not acknowledge the executor dispatch. The storage is correct. Reconcile or retry by `run_id`; do not record a storage rejection. The original `ValkyrieAPIError` is the cause.
- Plain `ValkyrieRunError` with a `run_id` means the start response carried a missing or different `storage_bucket`. Treat storage as unconfirmed and do not submit another run automatically.

Catch `ValkyrieRunAcceptedError` before its base class, `ValkyrieRunError`. Input errors can also raise `ValkyrieRunError`, with no `run_id`.

`DatasetViewRun.source_bucket` is selected per run. A view may read old runs from shared storage and new runs from different owner buckets. `DatasetViewRequest.destination_bucket` remains the dataset bucket. A null legacy per-run column uses ValSmith's documented legacy location. Publication must not replace a saved source with an owner's current bucket.

The provisioner must set `valsmith:valkyrie-org-id` to the canonical Valkyrie organization UUID before admission. ValSmith owns its two per-run columns, database migrations, provisioning, SDK pin, publication, and Lambda source selection. This repository makes none of those changes.

Use a reviewed immutable SDK commit that includes `ValkyrieRunAcceptedError`, the optional structured `ValkyrieRunError.run_id`, and the `SingleBenchmarkResponse.storage_bucket` field. Registry IAM and external Lambda/OIDC policy deployments must precede owner writes; ValSmith application rollout must follow the compatible tracker, host, and executor-release cutover.

## Verification and remaining release gates

Local tests cover saved locations with fake AWS operations and disposable PostgreSQL databases. They cannot prove deployed IAM or account configuration. Before production writes, complete the non-production smoke run through bundle freeze, terminal results, publication, download, and recovery. Record the tracker and host revisions, immutable executor artifact and protocol, SDK commit, and deployed IAM/account settings. Cloud deployments, service-role changes, and production enablement remain separate operator release gates.
