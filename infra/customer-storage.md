# Production customer storage contract

This feature is disabled by default. It is available only in stage `prod`.
`bench` remains separate even though `Stage.is_production` includes it.
The construct creates roles and backups. It does not create customer buckets,
a Lambda function, an OIDC provider, or application database resources.

## Inputs

Set the following deployment environment values before synthesis. Validation
uses only these inputs and the CDK target account. It does not query AWS or infer
an account from an AWS profile.

| Input | Required value |
| --- | --- |
| `VALSMITH_CUSTOMER_STORAGE_ENABLED` | `true` to enable; omitted or `false` to disable |
| `PRODUCTION_ACCOUNT_ID` | Explicit 12-digit external production account, equal to the CDK target |
| `VALSMITH_STORAGE_ORG_ID` | One canonical nonzero ValSmith organization UUID |
| `VALSMITH_STORAGE_OIDC_PROVIDER_ARN` | Existing `arn:aws:iam::<target-account>:oidc-provider/oidc.vercel.com/vals-ai` |
| `VALSMITH_STORAGE_OIDC_AUDIENCE` | Exactly `https://vercel.com/vals-ai` |
| `VALSMITH_STORAGE_OIDC_SUBJECT` | Exactly `owner:vals-ai:project:valsmith:environment:production` |
| `VALSMITH_DATASET_VIEW_LAMBDA_NAME` | Exact function name in the target account and region |
| `VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN` | One explicit IAM operator role ARN; no wildcard or account-root principal |
| `VALSMITH_LEGACY_STORAGE_BUCKET` | Exact shared compatibility bucket; never an owner bucket |
| `VALSMITH_LEGACY_STORAGE_ACCOUNT_ID` | Explicit account that owns that compatibility bucket; must differ from `PRODUCTION_ACCOUNT_ID` |

The compatibility bucket must stay in a separate account. The cutover phase 3
gate narrows these roles with a resource policy on that bucket, and a resource
policy only binds across accounts: inside one account the identity grant alone
already allows the read, so the reviewed narrowing would have no effect.

## Where the pipeline reads each input

Every input above is carried by the two jobs that synthesize `ValkProdSharedStack`,
`deploy-prod-core` and `executor-prod`, both of which run in the `prod-external`
GitHub Environment. Define the values on that Environment. No other job references
them, so a value defined at repository scope still cannot reach a `bench` or `dev`
synthesis, where an enabled flag would be refused for the wrong stage.

| Input | GitHub source |
| --- | --- |
| `VALSMITH_CUSTOMER_STORAGE_ENABLED` | Variable `VALSMITH_CUSTOMER_STORAGE_ENABLED` |
| `PRODUCTION_ACCOUNT_ID` | Secret `VALKYRIE_PRODUCTION_ACCOUNT_ID` |
| `VALSMITH_STORAGE_ORG_ID` | Secret `VALSMITH_STORAGE_ORG_ID` |
| `VALSMITH_STORAGE_OIDC_PROVIDER_ARN` | Secret `VALSMITH_STORAGE_OIDC_PROVIDER_ARN` |
| `VALSMITH_STORAGE_OIDC_AUDIENCE` | Variable `VALSMITH_STORAGE_OIDC_AUDIENCE` |
| `VALSMITH_STORAGE_OIDC_SUBJECT` | Variable `VALSMITH_STORAGE_OIDC_SUBJECT` |
| `VALSMITH_DATASET_VIEW_LAMBDA_NAME` | Secret `VALSMITH_DATASET_VIEW_LAMBDA_NAME` |
| `VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN` | Secret `VALSMITH_LIFECYCLE_OPERATOR_ROLE_ARN` |
| `VALSMITH_LEGACY_STORAGE_BUCKET` | Secret `VALSMITH_LEGACY_STORAGE_BUCKET` |
| `VALSMITH_LEGACY_STORAGE_ACCOUNT_ID` | Secret `VALSMITH_LEGACY_STORAGE_ACCOUNT_ID` |

Account IDs, ARNs, organization UUIDs and resource names follow their siblings in
this workflow, which are secrets. The two pinned OIDC literals and the boolean
follow `AWS_MANAGED_STORAGE_SUBMISSIONS_ENABLED` and `SANDBOX_QUEUE_ENABLED`,
which are variables. None of these values is a credential; the classification
only matches existing practice and keeps them out of build logs.

The enable flag has no workflow default. An undefined variable reaches the
synthesis as an empty string, which `from_environment` refuses, and the
`Validate prod deployment inputs` step refuses it earlier with a named error.
That is deliberate: a defaulted `false` would let one ordinary push to `prod`
synthesize the shared stack without this construct and delete the deployed roles,
both backup plans, both selections and the vault access policy, while the vault
and its key survive as orphans. The same step refuses an enabled flag with any
one of the other inputs empty, so a partly configured Environment fails before
any AWS call instead of deploying half the boundary.


The existing deployment preflight also requires separate bench/dev/production
account identities and region inputs. Do not reuse bench's account because a
profile has a production name. The compatibility source bucket needs a separate
resource policy that permits the destination application and Lambda roles to read
the approved keys. This stack cannot grant access in the source account.

## Outputs

Read these exact `ValkProdSharedStack` CloudFormation outputs after deployment:

- `CustomerStorageVaultName`
- `CustomerStorageVaultArn`
- `CustomerStorageBackupRoleArn`
- `CustomerStorageSystemBackupRoleArn`
- `CustomerStorageApplicationRoleArn`
- `CustomerStorageLambdaRoleArn`
- `CustomerStorageLifecycleRoleArn`
- `CustomerStorageAllowedOrgEnvironments`

The last output is JSON with the exact configured organization UUID mapped to
`["prod"]`. Apply it to the managed storage environment configuration from the
storage rollout. Verify organization membership separately. Storage inputs and
roles do not replace the application's database role or database target.

## Backup scope

The retained vault is `valsmith-customer-storage-prod`. It uses a retained,
rotating KMS key, has no Vault Lock, and has no deletion-deny policy.
The daily rule starts at 03:00 UTC and retains periodic recovery points for
30 days. It has no cold transition or continuous/PITR mode.

The daily rule sets `RecoveryPointTags` to exactly `valsmith:backup=true` and
`valsmith:environment=prod`. These are the two tags the vault access policy
requires before the lifecycle role can delete a recovery point. Every on-demand
`StartBackupJob` into this vault must send the same two `RecoveryPointTags`.
A recovery point without them cannot be deleted by any principal this construct
creates, so a departing owner's backup data would survive its deletion request.
Do not rely on AWS Backup to copy the source bucket's tags onto the recovery
point. A template test compares the rule tags with the vault policy condition.

The owner selection uses `arn:<partition>:s3:::vs-prod-*` and requires both
`valsmith:backup=true` and `valsmith:environment=prod`. The role's account condition
and foreign-account deny enforce ownership; S3 bucket ARNs have no account field.
New matching buckets need no CDK update. The production system bucket has a
separate explicit selection under a separate plan, `valsmith-system-prod`,
because recovery-point tags are a property of the plan rule. That plan sets no
recovery-point tags, so the lifecycle role cannot delete system recovery points.
CDK creates no owner buckets and adds no object expiration rule. Keep bucket versioning, SSE-S3, ownership enforcement, public
access blocking, and TLS protection enabled in the owner provisioner.

Each selection has its own AWS Backup service role. `ValSmithBackup-prod` serves
the owner selection and its S3 data grants cover only `vs-prod-*`.
`ValSmithSystemBackup-prod` serves the system selection and its S3 data grants
cover only the exact system bucket. Neither role can read the other's buckets.
The separation is what stops the lifecycle role from backing up the system bucket
into this vault under the owner recovery-point tags and then deleting that point:
the only role it may pass to AWS Backup is the owner role, which cannot read the
system bucket, so such a job fails. A recovery-point tag alone is not an
authorization boundary, because the caller of `StartBackupJob` chooses it.

Both roles follow the current S3 backup policy, including
`s3:ListTagsForResource` and `backup:TagResource`, both are assumable only by
`backup.amazonaws.com`, and both are account-bound by `s3:ResourceAccount`. KMS
grants cover only this vault key. There is no grant to arbitrary source KMS keys;
selected S3 objects must use SSE-S3. Backup EventBridge grants cover only
`AwsBackupManagedRule*` in the target account and region.

The installed CDK version treats selection `Conditions` as raw JSON. The
construct uses explicit CloudFormation capitalization to keep the two conditions
as AND conditions. Template tests check that shape.

## Role boundaries

`ValSmithStorage-prod` trusts only the exact configured Vercel production subject.
It can create and protect target owner buckets, list owner buckets for the
provisioner's HEAD and emptiness checks, read approved named roots and UUID run
roots, write named ValSmith roots, and invoke the configured Lambda. It cannot
delete object versions, delete buckets, manage backups, or access databases.
Explicit owner deletion denies on the application and Lambda roles prevent
bucket policies from granting those deletion actions back. The application still
needs `PutBucketPolicy` for TLS protection. Its freeze guards must prevent
provisioning from replacing an active lifecycle write fence.

`PutBucketPolicy` on `vs-prod-*` is a known residual exposure. IAM can bound the
bucket set and the account, but it cannot bound the content of a policy this role
writes. A compromised or faulty Vercel production deployment could therefore
grant `s3:GetObject` on one owner bucket to a foreign account, or drop the TLS
statement. No narrowing was available: S3 does not accept `aws:ResourceTag` on
bucket-level calls, `s3:ResourceAccount` is already applied and does not limit
what a written policy grants, and a separate provisioning role would still have
to be assumable by the same Vercel identity. Compensating controls: the
unconditional owner deletion deny stays on this role; removal of an active write
fence is detected, not accepted, by `TransferAWSBoundary.verify_source_fence`,
`RelocationAWSBoundary.verify_objects` and the whole-policy digest in
`AWSProviderBoundary.verify_fence`; and ValSmith refuses to re-provision a frozen
owner. Splitting provisioning from the run-time data plane needs a paired ValSmith
change and a spec decision; it is not in this slice.

The legacy read grant is wider than ValSmith's own data. `_read` grants
`s3:GetObject`, `s3:GetObjectVersion` and prefix-scoped listing on the four named
application roots **and** on `benchmarks/<run UUID>/*` in the compatibility
bucket. That bucket is the shared Valkyrie bucket, where every tenant's runs use
the same key shape, so IAM in this account cannot separate ValSmith's legacy runs
from another tenant's. The narrowing must come from the compatibility bucket's own
resource policy in the source account, which must list only the migration
cohort's run prefixes. The cutover runbook makes that a deployment gate. A
template test pins the exact five key scopes, so a further widening fails the
build.

`ValSmithDatasetView-prod` trusts only Lambda. It reads the approved owner and
explicit legacy roots, writes only `benchmarks/valsmith-dataset-views/` in owner
buckets, and writes logs only for its configured function. Trajectories and
patches published by the view code are below this view root.

`ValSmithLifecycle-prod` trusts only the configured operator role. It can inspect
and change policies on account-owned owner buckets, copy/read/delete exact object
versions, abort multipart uploads, delete empty owner buckets, and inspect/delete
UUID run log groups below `/valkyrie/benchmarks-prod/`. Removing a write fence
means restoring the reviewed TLS bucket policy with `PutBucketPolicy`; the role
has no `DeleteBucketPolicy` permission. It cannot delete the shared system bucket
or mutate the legacy shared source. Source migration and deletion use a separate
source role/client with exact reviewed resource grants. A foreign `vs-prod-*`
bucket remains denied even during cross-account migration.
Separate clients alone do not authorize S3 `CopyObject`: its caller needs both
source-read and destination-write access. The future migration transport must use
source-authenticated `GetObject` and stream its bytes into destination-authenticated
`PutObject` or multipart upload. Keep the foreign-owner denies on both roles.

The lifecycle role can start backups only into this vault and pass only
`ValSmithBackup-prod` to `backup.amazonaws.com`. It cannot pass
`ValSmithSystemBackup-prod`, and it has no `backup:TagResource`, no
`backup:StartRestoreJob` and no `backup:StartCopyJob`, so it has no other path to
system data. Recovery-point deletion is granted by this vault's resource policy,
only to the lifecycle role and only for points with the owner backup/environment
tags. No identity-wide recovery-point deletion grant exists. The owner plan
applies those two tags to every recovery point it creates, and every on-demand
`StartBackupJob` must apply them too, passing `ValSmithBackup-prod`. The separate
system plan applies no recovery-point tags, so its recovery points do not receive
that deletion grant. Never put owner backup tags on the system bucket or on a
system recovery point.

AWS Backup publishes no condition key for a recovery point's source resource. Its
whole condition-key set is `aws:RequestTag/${TagKey}`, `aws:ResourceTag/${TagKey}`,
`aws:TagKeys`, `backup:ChangeableForDays`, `backup:CopyTargetOrgPaths`,
`backup:CopyTargets`, `backup:FrameworkArns`, `backup:Index`,
`backup:MaxRetentionDays`, `backup:MinRetentionDays` and
`backup:MpaApprovalTeamArn`, and the `recoveryPoint` resource type accepts only
`aws:ResourceTag/${TagKey}`. The vault policy therefore cannot require the source
to be an owner bucket; the role split has to carry that boundary. The same table
lists no action-level condition key for `StartBackupJob`, so an IAM condition on
`aws:RequestTag/valsmith:backup` would never match and would deny every on-demand
backup. Do not add one.

Backup access points must be deleted through `DeleteBackupAccessPoint`, never
through direct S3 cleanup. The operator tool must verify the exact recovery-point
attachment and wait for deletion before deleting the recovery point. The role has
the current Backup inventory/describe/delete actions and their dependent S3
access-point permissions on `accesspoint/*` in the exact destination account and
region. Existing access-point names are accepted. IAM cannot express the
recovery-point attachment for the dependent S3 actions. This is a dedicated
operator permission boundary; the tool must check bucket ARN, vault, account,
region, and recovery-point attachment before each change. Foreign-account points
remain pending until a separate approved role can handle them. A supported operator SDK is required; an old client
without these APIs is a failed capability check, not an empty inventory.

Some AWS actions do not support resource-level permissions. These use resource
`*`, limited to the target region: `s3:ListAllMyBuckets`, `cloudwatch:GetMetricData`,
`events:ListRules`, `backup:ListBackupJobs`, `backup:ListCopyJobs`,
`backup:DescribeBackupJob`, `backup:DescribeCopyJob`,
`backup:ListRecoveryPointsByResource`,
`backup:ListBackupAccessPointsByRecoveryPoint`,
`backup:ListBackupAccessPointsByResource`, and `logs:DescribeLogGroups`.
They expose account-local inventory; they grant no global data deletion.
The Backup service authorization table lists access-point inventory as unscoped,
although the developer guide's older table shows a recovery-point ARN.

## Operator deployment gates

No deployment or live backup is verified by local synthesis. Before enabling
writes, the operator must verify the external account/region, existing Vercel
provider and exact subject, Lambda deployment and execution role, source read
policy, allowed organization/environment map, S3 bucket quota, and S3 opt-in in
AWS Backup. Use the outputs above for backup/lifecycle configuration. Check one
completed recovery point and a deliberate restore before source removal.
The separate cutover procedure controls tracker history, application URI updates,
source deletion, release order, and rollback. Keep intake frozen on a failed gate.

## AWS references

- [S3 backup permissions, current managed policy](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/AWSBackupServiceRolePolicyForS3Backup.html)
- [Backup selection conditions](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-backup-backupselection-backupselectionresourcetype.html)
- [Backup service authorization and supported resource types](https://docs.aws.amazon.com/service-authorization/latest/reference/list_backup.html)
- [Backup service reference information, the machine-readable form of that table](https://servicereference.us-east-1.amazonaws.com/v1/backup/backup.json)
- [Backup access-point dependent permissions](https://docs.aws.amazon.com/aws-backup/latest/devguide/access-control.html)
- [Vault resource policies](https://docs.aws.amazon.com/aws-backup/latest/devguide/create-a-vault-access-policy.html)
- [Backup encryption and key permissions](https://docs.aws.amazon.com/aws-backup/latest/devguide/encryption.html)

## Operator-call permission alignment

Application provisioning must inspect complete bucket emptiness with
`ListBucketVersions` and `ListBucketMultipartUploads`, in addition to `ListBucket`
for HeadBucket. These owner grants have no prefix filter and remain guarded by
`s3:ResourceAccount`; ordinary data reads/writes retain their approved roots.
The lifecycle role includes `GetBucketOwnershipControls` for immutable archive
validation. Exact object/version/tag reads and bounded owner writes remain scoped
to the configured account. Application and Lambda still cannot delete owner data
or manage Backup.

Lifecycle CloudWatch grants include CreateLogGroup, CreateLogStream, PutLogEvents,
PutRetentionPolicy, DescribeLogStreams, GetLogEvents, FilterLogEvents, Unmask and
DeleteLogGroup only for UUID-shaped `/valkyrie/benchmarks-prod/<run UUID>` groups in
the destination account/region. Runtime creation/writing supports future logs;
old logs remain in verified immutable archives. Only lifecycle authority can
remove an exact source log group after final proof. Lambda grants stay on its own
configured function log group.

The **separate source role** must permit STS GetCallerIdentity; bucket HeadBucket
(ListBucket), GetBucketLocation, GetBucketVersioning, GetBucketTagging,
GetBucketOwnershipControls for managed archives, GetBucketPolicy and narrowly
scoped PutBucketPolicy for the exact source fence; ListBucketVersions and
ListBucketMultipartUploads for reviewed prefixes; GetObject/GetObjectVersion and
GetObjectTagging/GetObjectVersionTagging for exact copied versions. Cleanup also
needs DeleteObjectVersion and AbortMultipartUpload on reviewed prefixes. Source
CloudWatch requires account-local DescribeLogGroups and exact saved-group
DescribeLogStreams, FilterLogEvents and explicit Unmask authority. Grant
DeleteLogGroup only to the cleanup lifecycle principal. Source provider drain
requires GetSecretValue on each exact saved provider secret, and scoped KMS
Decrypt only for an actual customer-key dependency. Planning and historical
transport do not export secret values.

Destination portable-reference verification also needs DescribeSecret on the
reviewed exact destination secret ARNs, plus exact immutable S3 reads. These
operator-selected secret prerequisites are separate from this construct, which
has no configured provider secret scope and grants no secret wildcard. Verify
runtime-role use separately. STS GetCallerIdentity is an identity check, not a
resource data grant. Source IAM, external bucket policies, secret grants and CI
settings are operator prerequisites; this construct does not modify them.

Follow the ordered [cutover runbook](../docs/deployment/customer-storage-cutover.md).
