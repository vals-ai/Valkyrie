# Production customer storage cutover

**Legacy log completion runs under the quiet-interval policy.** The paired Tracker production boundary accepts a legacy source log group as complete only when the run is terminal, every dispatch drain is non-pending with its newest drain time older than the quiet interval (the observed exit, the external host observation, or the contract's `acknowledgement_required_since` for a dispatch that was never claimed or that a verified contract reported finished), the lifecycle hold is older than the quiet interval, the first frozen scan's newest event and newest ingestion times are older than the quiet interval, and both frozen scans match. The interval is `TRANSFER_LOG_QUIET_INTERVAL_HOURS`, default and minimum 24 hours, measured against the validated host-contract observation time. Accepted risk: a CloudWatch event that AWS accepted but that `FilterLogEvents` still does not return 24 hours after the last publisher stopped is lost when the source log group is deleted. See [the transfer policy](tracker-transfer.md).

This is an operator procedure. Local tests do not prove deployment, account access, data migration, a completed backup, or a working application. The external production account, organization and operator identities require verified inputs. A profile name is not identity evidence. Keep an operations record with exact release revisions, plans, file hashes, fresh reports, approved identities and gate results; keep credentials and customer payloads out of that record.

Merge the reviewed storage and lifecycle foundations, then the relocation corrections, then this Valkyrie production slice. Merge the separate ValSmith migration/deletion corrections and final production adapter before applying cutover. The adapter must consume the exact versioned contracts below without runtime imports from this repository. Its mixed-location and paired-database tests are a separate delivery gate. The service-registry reader IAM and destination Lambda deployment are also required dependencies. No merge or deployed revision is established by this document.

## 1. Verify identities, targets and runtime compatibility

Record source and destination AWS account IDs and regions, both Tracker database targets and URLs, the ValSmith database target, the exact organization UUID, every affected GitHub owner ID, and both repository revisions. Run `aws sts get-caller-identity` separately with the source and destination credentials. Compare both `Account` and `Arn` to the approved record before any mutation; stop on a missing value or mismatch. Retain separate source and destination credentials throughout the procedure. Do not change a foreign-account deny to make a copy succeed.

Confirm the Vercel provider exists in the destination account and permits exactly audience `https://vercel.com/vals-ai` and subject `owner:vals-ai:project:valsmith:environment:production`. Verify actual service discovery, provider-secret locators and each destination executor-release mapping. The external Tracker must include current saved-runtime persistence, hold enforcement and protocol-v3 code; old production code is not a valid target. Compare the actual running binary revisions with the approved revisions.

ValSmith uses one `TRACKER_SERVICE_URL` and one storage role for the deployment. Inventory **all affected owners and historical run locations served by that deployment** before changing them. A completed owner import does not authorize a global switch past owners that remain on the source. Record a complete migration cohort and a separate validation owner that already uses only destination resources.

Keep `AWS_ROLE_ARN` and the exact ValSmith schema/database target independent from `VALSMITH_S3_ROLE_ARN`. Current ValSmith `AWS_REGION`/`AWS_DEFAULT_REGION` supplies both database-token and S3/Lambda clients. Require a storage/Lambda destination region compatible with the existing database target, or complete a separately reviewed region-separation change first. A paired Tracker transfer can name different regions; that does not prove the current ValSmith application can use them.

## 2. Verify S3 quota and capacity

Use destination credentials to run `aws service-quotas list-service-quotas --service-code s3` and identify the general-purpose bucket quota and its returned `QuotaCode`. Use `get-service-quota` for that code. Inventory existing destination buckets and count planned owner buckets plus system/validation capacity. Use the returned quota code in `request-service-quota-increase` when needed. Record the request ID and granted quota; wait for approval before provisioning. Do not reuse bench's observed quota as destination evidence. Bucket names are globally unique: record the real destination names and prove their availability/account ownership.

## 3. Deploy schema, protection, roles and compatible runtime

Deploy additive Tracker migrations through `0e1f2a3b4c5d` and the corresponding reviewed ValSmith additive schema. Deploy compatible stable hosts that enforce holds and acknowledge actual process exits, then Tracker guards/readers/operator commands and protocol-v3 executor releases. Inventory every old host and dispatch. Keep lifecycle apply disabled until the host contract is verified.

**The protocol-v3 push is a maintenance deploy that stops live work.** `MANAGED_EXECUTION_PROTOCOL_VERSION` moves from `"2"` to `"3"` in `services/tracker/src/executor_protocol.py`. `infra/classify_repository_change.py` lists that file as both an executor-stack file and an executor-release file, and `.dockerignore` copies it into the executor-host image, so the host image and the release always move together. This slice also changes `infra/shared.py` and `infra/stage_config.py`, which are executor-shared files, so `core_maintenance_required` is true, `deploy-prod-core` does not run, and `executor-prod` owns the whole sequence: begin maintenance, deploy the core stacks, deploy the executor stack, publish and activate the release, finish maintenance. The classification is `maintenance-required`, so the merge waits for the `maintenance-prod` Environment approval.

Plan for the window `begin_maintenance` opens. It sets `executoradmission.maintenance_target_sha`, moves every `IN_PROGRESS` or `STOPPING` run and every active task to `STOPPED` and every `QUEUED` or `RUNNING` dispatch to `FAILED`, then sets the executor-host and Tracker services to `desiredCount=0` and force-stops the remaining host tasks (`services/tracker/src/tracker/executor/release_entrypoint.py`). Every run still in flight is lost and the Tracker is unavailable for the whole deploy. Submissions fail at the load balancer while the Tracker is at zero, and with 503 from the admission fence otherwise, because `select_active_release` raises `MaintenanceModeError` while the fence is set. Complete the phase 4 freeze and drain before the push instead of letting the deploy stop live work.

Confirm the hosts caught up before intake reopens. `finish_maintenance` restores both desired counts and waits on the ECS `services_stable` waiter for both services, so a successful `Finish prod maintenance` step is the first evidence. Check it directly too:

```bash
aws ecs describe-services --cluster AgenticHarnessCluster-prod --services ExecutorHost-prod \
  --query 'services[0].[taskDefinition,desiredCount,runningCount,deployments[0].rolloutState]'
aws ecs list-tasks --cluster AgenticHarnessCluster-prod --service-name ExecutorHost-prod \
  --query taskArns --output text \
  | xargs aws ecs describe-tasks --cluster AgenticHarnessCluster-prod --query 'tasks[].taskDefinitionArn' --tasks
```

Require `rolloutState` `COMPLETED`, `runningCount` equal to `desiredCount`, and every running task on the task-definition revision the service now names. Then confirm the active release with read-only Tracker database credentials:

```sql
SELECT release.id, release.protocol_version, release.status, release.readiness_verified,
       admission.maintenance_target_sha
FROM executoradmission AS admission
JOIN executorrelease AS release ON release.id = admission.release_id;
```

Require `protocol_version = '3'`, `status = 'ACTIVE'`, `readiness_verified = true` and `maintenance_target_sha IS NULL`. A managed run submitted while the active release is still protocol 2 is refused with 503, naming `Activate an executor release that supports managed runs`; it is not accepted and then left to expire. No dispatch can carry protocol 3 before that release is active, because `create_executor_dispatch` stamps the active release's own `protocol_version` rather than the Tracker's constant, so no stale host is ever handed a payload it cannot read.

Enable the production-only construct with `VALSMITH_CUSTOMER_STORAGE_ENABLED=true`. Supply every validated input from [the infrastructure contract](../../infra/customer-storage.md): `PRODUCTION_ACCOUNT_ID`, `VALSMITH_STORAGE_ORG_ID`, `VALSMITH_STORAGE_OIDC_PROVIDER_ARN`, `VALSMITH_STORAGE_OIDC_AUDIENCE`, `VALSMITH_STORAGE_OIDC_SUBJECT`, `VALSMITH_DATASET_VIEW_LAMBDA_NAME`, `VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN`, `VALSMITH_LEGACY_STORAGE_BUCKET`, and `VALSMITH_LEGACY_STORAGE_ACCOUNT_ID`. Leave nonproduction defaults disabled.

Read these exact outputs from `ValkProdSharedStack`: `CustomerStorageVaultName`, `CustomerStorageVaultArn`, `CustomerStorageBackupRoleArn`, `CustomerStorageSystemBackupRoleArn`, `CustomerStorageApplicationRoleArn`, `CustomerStorageLambdaRoleArn`, `CustomerStorageLifecycleRoleArn`, and `CustomerStorageAllowedOrgEnvironments`. Verify the vault is retained, encrypted, has no Vault Lock, and has the daily 03:00 UTC periodic plan with 30-day expiry. Verify both AND owner tags and the separate system-bucket selection. Do not add noncurrent object expiry or owner tags to the system bucket.

**Gate: separate backup roles.** The owner selection `valsmith-owners-prod` must show `IamRoleArn` equal to `CustomerStorageBackupRoleArn` (`ValSmithBackup-prod`), and the system selection `valsmith-system-prod` must show `CustomerStorageSystemBackupRoleArn` (`ValSmithSystemBackup-prod`). The two values must differ. Read both with `aws backup list-backup-selections` and `get-backup-selection` for each plan. Confirm with `aws iam list-role-policies` and `get-role-policy` that `ValSmithBackup-prod` grants S3 reads only on `vs-prod-*`, that `ValSmithSystemBackup-prod` grants them only on the exact system bucket, and that the `iam:PassRole` statement on `ValSmithLifecycle-prod` names `ValSmithBackup-prod` and nothing else. This is what keeps the lifecycle role from backing up the system bucket under the owner recovery-point tags and then deleting that recovery point; the tags alone cannot do it, because the caller of `StartBackupJob` chooses them.

**Gate: recovery-point tags.** The owner plan `valsmith-customer-storage-prod` must show `RecoveryPointTags` of exactly `valsmith:backup=true` and `valsmith:environment=prod`, and the system plan `valsmith-system-prod` must show none. Every on-demand `backup:StartBackupJob` into this vault, including the ones ValSmith issues before a deletion, must send `RecoveryPointTags={"valsmith:backup": "true", "valsmith:environment": "prod"}` and must pass `IamRoleArn=CustomerStorageBackupRoleArn` (`ValSmithBackup-prod`), never the system backup role. The vault access policy allows `backup:DeleteRecoveryPoint` only for recovery points that carry both tags, so an untagged recovery point cannot be deleted by any role in this stack and would keep a departing owner's object data for up to 30 days. Read the tags back with `aws backup describe-recovery-point` before phase 8, and never add these tags to a system recovery point.

**Gate: compatibility bucket policy.** `ValSmithStorage-prod` and `ValSmithDatasetView-prod` can read and list `benchmarks/<run UUID>/*` in the compatibility bucket named by `VALSMITH_LEGACY_STORAGE_BUCKET`. That bucket holds every Valkyrie tenant's runs under the same key shape, so the destination IAM policy cannot separate ValSmith's legacy runs from another tenant's. Before these roles are used, add a reviewed statement to the compatibility bucket's own resource policy, in its own account, that allows those two role ARNs only the enumerated run prefixes of the migration cohort and the four named `benchmarks/valsmith-*` roots.

**Add the statement; never replace the document.** `aws s3api put-bucket-policy` replaces the entire policy, and this bucket is shared by every Valkyrie tenant. Writing a freshly authored document there removes whatever is already in place, including TLS denial, cross-account grants and log-delivery grants, for every tenant of the bucket. This repository has no helper that writes a bucket policy: `TransferAWSBoundary.verify_source_fence`, `AWSProviderBoundary.verify_fence` and `RelocationAWSBoundary.verify_objects` only read, check one `Sid` and digest the whole document. So read, merge and verify by hand, with source-account credentials throughout.

Read the current document and keep it:

```bash
aws s3api get-bucket-policy \
  --bucket "$VALSMITH_LEGACY_STORAGE_BUCKET" \
  --expected-bucket-owner "$VALSMITH_LEGACY_STORAGE_ACCOUNT_ID" \
  --query Policy --output text > current-policy.json
```

A `NoSuchBucketPolicy` error means the bucket carries no policy at all, which on a shared production bucket also means it has no TLS denial. Stop and confirm that with the owning account before you treat the absence as real; do not invent a document.

Merge the reviewed statements into the current document with the repository interpreter, `uv run --project services/tracker python`. Put only the new statements in `reviewed-statements.json`, each with its own unique `Sid`:

```python
import hashlib
import json
from pathlib import Path

def digest(policy: object) -> str:
    return hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

current = json.loads(Path("current-policy.json").read_text())
added = json.loads(Path("reviewed-statements.json").read_text())
added_sids = [statement["Sid"] for statement in added]
existing_sids = {statement.get("Sid") for statement in current["Statement"]}
if len(set(added_sids)) != len(added_sids) or existing_sids & set(added_sids):
    raise SystemExit("A reviewed Sid already exists on this bucket; resolve it by hand.")

merged = {**current, "Statement": [*current["Statement"], *added]}
Path("merged-policy.json").write_text(json.dumps(merged, indent=2))
print(json.dumps({"before": digest(current), "after": digest(merged), "added": added_sids}, indent=2))
```

That digest is the one `policy_digest` in `services/tracker/src/tracker/run_purge/providers.py` computes, so the value you record is the value the lifecycle tooling recomputes later. Diff `current-policy.json` against `merged-policy.json` and confirm the only difference is the appended statements. A merged document over 20 KB is refused by S3, so a cohort prefix list that does not fit must be reduced before the write, not trimmed from the existing statements.

Write it, then read it back and prove nothing else moved:

```bash
aws s3api put-bucket-policy \
  --bucket "$VALSMITH_LEGACY_STORAGE_BUCKET" \
  --expected-bucket-owner "$VALSMITH_LEGACY_STORAGE_ACCOUNT_ID" \
  --policy file://merged-policy.json

aws s3api get-bucket-policy \
  --bucket "$VALSMITH_LEGACY_STORAGE_BUCKET" \
  --expected-bucket-owner "$VALSMITH_LEGACY_STORAGE_ACCOUNT_ID" \
  --query Policy --output text > readback-policy.json
```

The readback must keep every original statement in its original order and content, and its extra statements must be exactly the reviewed ones. Confirm that `json.loads(readback)["Statement"][: len(current["Statement"])] == current["Statement"]` and that the remainder equals `added`, and that every statement naming either role ARN lists only the cohort's run prefixes and the four `benchmarks/valsmith-*` roots. Record the before digest, the after digest, the readback digest, the added `Sid` values, both policy documents and the reviewer in the operations record, under "compatibility bucket policy". If the readback loses any original statement, restore `current-policy.json` with the same `put-bucket-policy` call before doing anything else. If the cohort cannot be enumerated, record an explicit accepted-risk entry in the same place, naming the wider scope and its approver; do not continue without one of the two.

Run `aws backup describe-region-settings` in the destination region. Explicitly opt S3 into Backup with `update-region-settings --resource-type-opt-in-preference S3=true`, then read settings again. Preserve unrelated settings. Check the actual role and backup selection access. Verify Tracker `AWS_DEPLOYMENT_ACCOUNT_ID`, `AWS_DEPLOYMENT_REGION`, `AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS` (from `CustomerStorageAllowedOrgEnvironments`) and controlled `AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED` together.

Deploy and verify service reader permissions and the destination dataset-view Lambda with `CustomerStorageLambdaRoleArn`. ValSmith's current `scripts/deploy_dataset_view_lambda.py:TARGETS` and `.github/workflows/deploy-dataset-view-lambda.yml` pin production to bench `613431292675`. Vercel storage variables do not redirect that CI path. The existing workflow updates code on an existing function; it does not create it or replace its role/configuration. The separate ValSmith slice must supply a reviewed destination deployment path with explicit identity checks and preserved defaults. Verify the actual destination function, model-gateway/secret configuration and future code-update path before proceeding. This runbook does not claim that function exists.

## 4. Freeze the complete cohort and prove drain

Restrict general intake and freeze every migration-cohort owner. Settle local leases, foreign-reference writers, remote runs, sandbox creation/cleanup, backup jobs and copy jobs. Preserve ValSmith's reviewed writer coordination and durable retired-source reference controls. Inventory all artifact aliases, owner references and downstream published copies before plan approval.

Generate read-only owner plans and exact Tracker inventories. Bind owner/org/account pair, regions, actual database targets, sorted run UUIDs, saved resources and immutable parent/child digests. Use a trusted absolute interpreter and script with fixed arguments, private mode-0600 JSON files, and credentials only in named environment variables. The documented repository interpreter is `uv run --project services/tracker python`. Never pass credentials in argv.

Host observations use `stable-host-lifecycle-v1`, a complete host inventory, deployment digest, named verifier and UTC observation time no more than 15 minutes old. Refresh them at each phase. Started legacy dispatches without positive exit evidence require the separate exact external host-drain evidence: disable old claims, terminate every relevant host, verify termination, and bind the evidence file digest to the operation, hold, dispatch IDs and host inventory. Failed status, expired leases and stop responses do not prove exit.

## 5. Import objects and historical Tracker state

Use [same-account relocation](tracker-relocation.md) only for same-account saved-bucket changes. A mixed owner includes every actual run. An already-destination run uses `location_policy=hold_only` with identical saved/destination resources, no copied source proof, complete current `destination_versions`, exact retained-version transformations, full holds/drain/reference checks and explicit execution policy. Do not omit that run from the safety boundary. The default `relocate` policy requires different buckets. A pre-existing immutable log archive currently blocks same-account relocation; stop before copy or location mutation until a separately reviewed archive remapper exists.

For production transfer use [the paired operator](tracker-transfer.md), with `docs/contracts/tracker-transfer-request-v1.schema.json`, `tracker-transfer-response-v1.schema.json` and `tracker-transfer-inspect-v1.schema.json`. Its fixed CLI arguments are `--request`, `--report`, `--source-database-url-env`, `--destination-database-url-env`, `--expected-source-database-target`, `--expected-destination-database-target`, `--source-aws-profile-env`, `--destination-aws-profile-env` and `--journal-directory`. The request selects `plan`, `prepare`, `inspect`, `import`, `cleanup` or `finalize`; only mutation actions use `--apply`. Both safe database targets are `postgresql:<host-or-socket>:<port>/<database>`. Require exit 0 and this invocation's fresh `nonce` before using a response.

**Gate: quiet interval.** Run `prepare` at least the quiet interval before `import`, and keep the cohort frozen for the whole of it. Record, per run, the hold acquisition time, the newest dispatch exit or external host observation, and the host-contract observation time used at import. A failing clause returns exit 2 and names itself in the error, and the command changes nothing.

Prepare source holds before stable inventory/copy. Verify the exact source prefix fence and separate owner freeze. Copy every version, delete marker and retained current state with separate source-authenticated reads and destination-authenticated writes. Preserve exact transformation evidence and complete destination history; source cleanup proof cannot serve as destination copy proof. Verify copied bytes, sizes, tags and exact version IDs. Reject incomplete pagination or multipart uploads.

Import preserves original run/task/result/dispatch identities and timestamps, organization scope, stored private fields and explicit release mappings. The destination organization and release catalog must already exist. The destination row set and its local hold commit together. Preserve historical logs through verified versioned manifests/chunks under each exact owner run prefix. Read every manifest/chunk by immutable version, check all aggregate counts/digests and exhaust the historical reader. Do not replay or re-date old CloudWatch events. Pre-existing source archives are an explicit planning restriction in paired transfer; never drop them.

Run fresh `inspect` and verify every row/table digest, source/destination hold identity/scope, current object history and archive. Keep source rows/logs, the owner freeze and both holds on every failed/incomplete transfer. ValSmith completes `waiting_transfer` only through the separately reviewed adapter, a fresh verified transfer and its final location transaction. Keep source cleanup as a later explicit phase.

## 6. Switch verified application configuration with intake restricted

Require successful import and common read checks for every owner/history location in the deployment-wide inventory. Apply the reviewed ValSmith configuration together: `VALSMITH_S3_ROLE_ARN=CustomerStorageApplicationRoleArn` (use the actual output value), `VALSMITH_STORAGE_AWS_ACCOUNT_ID`, `VALSMITH_STORAGE_ENVIRONMENT=prod`, `VALSMITH_STORAGE_VALKYRIE_ORG_ID`, `VALSMITH_OWNER_BUCKET_PREFIX=vs`, `VALSMITH_OWNER_BUCKETS_ENABLED`, `VALSMITH_MANAGED_RUN_STORAGE_ENABLED`, and `VALKYRIE_AWS_MODE=managed`. Use the actual reviewed `VALSMITH_S3_BUCKET` and `VALKYRIE_S3_BUCKET` compatibility/system locations. Set `TRACKER_SERVICE_URL`, `VALKYRIE_API_KEY`, `VALKYRIE_CUSTOM_BENCHMARK_SERVICES`, `VALKYRIE_SANDBOX_PROVIDER`, `VALKYRIE_SANDBOX_PROVIDER_SECRET_NAME` and `VALSMITH_DATASET_VIEW_LAMBDA` for the matching destination runtime. Record vault/backup/lifecycle output values in the private operator configuration.

Keep database-role selection and the schema target unchanged for a storage-only move. Keep every migration owner frozen and general intake restricted through the next phase. Do not delete source data merely because the settings were accepted.

**Accepted exposure: bucket policy writes from the web deployment.** `ValSmithStorage-prod` is assumed with Vercel web identity and holds `s3:PutBucketPolicy` on every `vs-prod-*` bucket in the destination account, because the same role provisions owner buckets and applies their TLS protection. IAM cannot limit the content of a policy that role writes, so a compromised or faulty production deployment could grant a foreign account read access to one owner bucket or drop its TLS statement. The compensating controls are: the unconditional owner deletion deny on that role; fence removal detected by `verify_source_fence`, `RelocationAWSBoundary.verify_objects` and the whole-policy digest in `AWSProviderBoundary.verify_fence`; and ValSmith's refusal to re-provision a frozen owner. Add a bucket-policy change alarm on the destination account for `vs-prod-*` before intake resumes, and read back each owner bucket policy in the phase 7 evidence. Splitting provisioning from the run-time data plane requires a paired ValSmith change and a spec decision; it is not in this release.

## 7. Verify controlled workflows, history and backups

Run one operator-approved public workflow and one private workflow under the designated validation owner **outside the migration cohort**. That owner must already use only destination resources. Record its owner ID, approved run UUIDs, limited admission control, permission review and cleanup obligations. This procedure adds no application bypass. Do not unfreeze a migrated owner to obtain the evidence required for its unfreeze.

Check returned saved run locations, owner manifests/views/artifacts, private/public access grants and current logs. Read historical runs for every frozen migrated owner through the application's authenticated history path, including old attempts, pagination, archive-only runs and archive plus current logs. Record exact run/version/count evidence without customer content.

Verify the protected-resource inventory, at least one completed owner recovery point in `CustomerStorageVaultArn`, and a deliberate restore/readback under approved isolated resources. Check the periodic selection and system coverage. A submitted or running backup job is not completion evidence. Failures keep the cohort frozen and source intact.

**Gate: owner bucket policies and their change alarm.** Phase 6 accepts that `ValSmithStorage-prod` can rewrite any `vs-prod-*` bucket policy from the Vercel web deployment. Close that here. For every owner bucket recorded for the cohort, named `vs-prod-<github login>-<github account id>` plus a collision suffix where one was assigned, run `aws s3api get-bucket-policy --bucket <recorded bucket name> --expected-bucket-owner <production account id>` with destination credentials, and record the returned policy digest in the operations record. Each policy must still deny insecure transport, must grant no principal outside the production account, and must carry no lifecycle write fence once the owner is unfrozen. Then confirm the change alarm that phase 6 requires exists and is in `OK`: an EventBridge rule on CloudTrail `PutBucketPolicy`, `DeleteBucketPolicy` and `PutBucketAcl` events for `vs-prod-*` in the destination account, with a subscribed notification target and one delivered test notification. Record the rule name, the target, the test notification time and every policy digest. A missing alarm or an unexplained policy difference keeps the cohort frozen.

## 8. Perform separately authorized exact source cleanup

Require the operation-bound finalization gate after all application and backup checks. Bind the exact ValSmith final location commit and object completion to `parent_completion.valsmith_commit_sha256` and `object_completion_sha256`, plus the independently checked sorted `destination_rows_sha256` and `archives_sha256`. Match operation, parent and child digests. Do not invent success flags.

The parent removes only exact copied source versions/markers after repeated destination verification and reference-writer coordination. Then paired `cleanup --apply` rechecks source object absence, current destination/archive readback and source rows/logs before removing the exact saved log groups and row closure. Keep `transferred_source_retired` active in the source database. `finalize --apply` releases only portable destination holds; `transferred_history_only` remains active for readable history. Same-account history retains `relocated_history_only`. Keep source bucket/provider inspection access until the final checks finish.

Later deletion may replace only an exact completed history predecessor under locks, without a release gap. Use `docs/deployment/tracker-purge-plan-v1.schema.json` and `tracker-purge-inspection-v1.schema.json`; `present_history_held` is distinct from unheld/deletion-held/removed. Inspection requires a fresh `--request-nonce` and preserves immutable expected labels. In-progress, retired-source and deletion holds cannot be replaced.

Handle source owner recovery points through the scoped lifecycle tools, with exact vault/bucket/account/region attachment checks and Backup access-point deletion through Backup APIs. Keep system backups and unrelated benchmark objects/bundles. Repeat the complete source/bench inventory, report unresolved and orphaned ValSmith keys, and retain the deletion ledger. Resolve downstream published copies under the separate deletion procedure. Never use a bucket-wide blind delete as verification.

## 9. Resume intake and record rollback limits

Unfreeze each migration owner only after the complete deployment-wide import, application-read, controlled workflow, backup and final cleanup gates pass. Verify retained source/history controls and the fresh final reports first. Resume general intake under the reviewed destination configuration. Clean up the validation owner's approved test data through the normal lifecycle procedure and retain the restricted evidence record.

Before source deletion, rollback uses preserved source locations, compatible binaries and reviewed reversal of destination/location changes while all affected owners remain frozen. Once any source version, log group or row is deleted, a flag/configuration reversal cannot restore it. Recovery requires a deliberate restore from verified backups/archives and the exact deletion ledger, followed by fresh identity, content and application checks. Do not point old code or settings at removed data.
