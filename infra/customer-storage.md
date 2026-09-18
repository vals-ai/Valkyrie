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
| `VALSMITH_LEGACY_STORAGE_ACCOUNT_ID` | Explicit account that owns that compatibility bucket |

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

The owner selection uses `arn:<partition>:s3:::vs-prod-*` and requires both
`valsmith:backup=true` and `valsmith:environment=prod`. The role's account condition
and foreign-account deny enforce ownership; S3 bucket ARNs have no account field.
New matching buckets need no CDK update. The production system bucket has a
separate explicit selection. CDK creates no owner buckets and adds no object
expiration rule. Keep bucket versioning, SSE-S3, ownership enforcement, public
access blocking, and TLS protection enabled in the owner provisioner.

The custom backup role follows the current S3 backup policy, including
`s3:ListTagsForResource` and `backup:TagResource`. S3 data grants cover only the
owner pattern and the exact system bucket in the target account. KMS grants
cover only this vault key. There is no grant to arbitrary source KMS keys;
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

The lifecycle role can start backups only into this vault and pass only the backup
role to `backup.amazonaws.com`. Recovery-point deletion is granted by this vault's
resource policy, only to the lifecycle role and only for points with the owner
backup/environment tags. No identity-wide recovery-point deletion grant exists.
The separate system selection does not add owner tags, so its recovery points do
not receive that deletion grant. Never put owner backup tags on the system bucket.

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
- [Backup access-point dependent permissions](https://docs.aws.amazon.com/aws-backup/latest/devguide/access-control.html)
- [Vault resource policies](https://docs.aws.amazon.com/aws-backup/latest/devguide/create-a-vault-access-policy.html)
- [Backup encryption and key permissions](https://docs.aws.amazon.com/aws-backup/latest/devguide/encryption.html)
