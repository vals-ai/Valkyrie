"""Production customer backups and separate application, Lambda, and operator roles."""

import json
from typing import cast

import aws_cdk as cdk
from aws_cdk import aws_backup, aws_iam, aws_kms, aws_s3
from constructs import Construct
from customer_storage_config import CustomerStorageConfig

# Run artifacts use UUID roots; application artifacts have named roots.
_RUN_ROOT = "benchmarks/????????-????-????-????-????????????/"
_APPLICATION_ROOTS = (
    "benchmarks/valsmith-manifests/",
    "benchmarks/valsmith-datasets/",
    "benchmarks/valsmith-dataset-views/",
    "benchmarks/valsmith-repositories/",
)
_READ_ROOTS = (*_APPLICATION_ROOTS, _RUN_ROOT)
_VIEW_ROOTS = ("benchmarks/valsmith-dataset-views/",)
_DATA_ACTIONS = [
    "s3:GetObject",
    "s3:GetObjectVersion",
    "s3:GetObjectTagging",
    "s3:GetObjectVersionTagging",
    "s3:GetObjectAcl",
    "s3:GetObjectVersionAcl",
    "s3:PutObject",
    "s3:PutObjectTagging",
    "s3:PutObjectVersionTagging",
    "s3:DeleteObject",
    "s3:DeleteObjectVersion",
    "s3:AbortMultipartUpload",
    "s3:ListMultipartUploadParts",
    "s3:ListBucket",
    "s3:ListBucketVersions",
    "s3:ListBucketMultipartUploads",
]


class CustomerStorage(Construct):
    def __init__(
        self, scope: Construct, construct_id: str, config: CustomerStorageConfig, system_bucket: aws_s3.IBucket
    ):
        super().__init__(scope, construct_id)
        stack = cdk.Stack.of(self)
        self.config = config
        self.owner_arn = f"arn:{stack.partition}:s3:::vs-prod-*"
        self.legacy_arn = f"arn:{stack.partition}:s3:::{config.legacy_bucket}"
        self.account_condition = {"StringEquals": {"s3:ResourceAccount": config.account_id}}
        self.recovery_point_arn = stack.format_arn(
            service="backup", resource="recovery-point", resource_name="*", arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME
        )
        self.access_point_arn = stack.format_arn(
            service="backup",
            resource="accesspoint",
            resource_name="*",
            arn_format=cdk.ArnFormat.SLASH_RESOURCE_NAME,
        )
        self.vault_name = "valsmith-customer-storage-prod"

        self.backup_role = aws_iam.Role(
            self,
            "BackupRole",
            role_name="ValSmithBackup-prod",
            assumed_by=cast(aws_iam.IPrincipal, aws_iam.ServicePrincipal("backup.amazonaws.com")),
        )
        self.application_role = aws_iam.Role(
            self,
            "ApplicationRole",
            role_name="ValSmithStorage-prod",
            assumed_by=cast(
                aws_iam.IPrincipal,
                aws_iam.FederatedPrincipal(
                    config.oidc_provider_arn,
                    {
                        "StringEquals": {
                            "oidc.vercel.com/vals-ai:aud": config.oidc_audience,
                            "oidc.vercel.com/vals-ai:sub": config.oidc_subject,
                        }
                    },
                    "sts:AssumeRoleWithWebIdentity",
                ),
            ),
        )
        self.lambda_role = aws_iam.Role(
            self,
            "LambdaRole",
            role_name="ValSmithDatasetView-prod",
            assumed_by=cast(aws_iam.IPrincipal, aws_iam.ServicePrincipal("lambda.amazonaws.com")),
        )
        self.lifecycle_role = aws_iam.Role(
            self,
            "LifecycleRole",
            role_name="ValSmithLifecycle-prod",
            assumed_by=cast(aws_iam.IPrincipal, aws_iam.ArnPrincipal(config.operator_role_arn)),
        )
        key = aws_kms.Key(self, "VaultKey", enable_key_rotation=True, removal_policy=cdk.RemovalPolicy.RETAIN)
        self.backup_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"],
                resources=[key.key_arn],
            )
        )
        self.backup_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["kms:CreateGrant"],
                resources=[key.key_arn],
                conditions={
                    "Bool": {"kms:GrantIsForAWSResource": "true"},
                    "StringEquals": {"kms:ViaService": f"backup.{stack.region}.{stack.url_suffix}"},
                },
            )
        )
        self.vault = aws_backup.BackupVault(
            self,
            "Vault",
            backup_vault_name=self.vault_name,
            encryption_key=key,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )
        # A vault policy grants deletion only in this vault; the role has no identity-wide delete grant.
        self.vault.add_to_access_policy(
            aws_iam.PolicyStatement(
                actions=["backup:DeleteRecoveryPoint"],
                resources=["*"],
                principals=[self.lifecycle_role],
                conditions={
                    "StringEquals": {
                        "aws:ResourceTag/valsmith:backup": "true",
                        "aws:ResourceTag/valsmith:environment": "prod",
                    }
                },
            )
        )
        plan = aws_backup.CfnBackupPlan(
            self,
            "Plan",
            backup_plan=aws_backup.CfnBackupPlan.BackupPlanResourceTypeProperty(
                backup_plan_name="valsmith-customer-storage-prod",
                backup_plan_rule=[
                    aws_backup.CfnBackupPlan.BackupRuleResourceTypeProperty(
                        rule_name="daily",
                        target_backup_vault=self.vault.backup_vault_name,
                        schedule_expression="cron(0 3 * * ? *)",
                        enable_continuous_backup=False,
                        lifecycle=aws_backup.CfnBackupPlan.LifecycleResourceTypeProperty(delete_after_days=30),
                    )
                ],
            ),
        )
        # CDK 2.237 treats Conditions as untyped JSON and otherwise emits lower-case keys.
        conditions = {
            "StringEquals": [
                {"ConditionKey": "aws:ResourceTag/valsmith:backup", "ConditionValue": "true"},
                {"ConditionKey": "aws:ResourceTag/valsmith:environment", "ConditionValue": "prod"},
            ]
        }
        for name, resources, selection_conditions in (
            ("owners", [self.owner_arn], conditions),
            ("system", [system_bucket.bucket_arn], None),
        ):
            selection = aws_backup.CfnBackupSelection(
                self,
                f"{name.title()}Selection",
                backup_plan_id=plan.ref,
                backup_selection=aws_backup.CfnBackupSelection.BackupSelectionResourceTypeProperty(
                    selection_name=f"valsmith-{name}-prod",
                    iam_role_arn=self.backup_role.role_arn,
                    resources=resources,
                    conditions=selection_conditions,
                ),
            )

            selection.node.add_dependency(self.backup_role)

        self._backup_permissions(system_bucket)
        self._application_permissions()
        self._lambda_permissions()
        self._lifecycle_permissions()
        for role in (self.backup_role, self.application_role, self.lambda_role, self.lifecycle_role):
            role.add_to_policy(
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.DENY,
                    actions=_DATA_ACTIONS,
                    resources=[self.owner_arn, f"{self.owner_arn}/*"],
                    conditions={"StringNotEquals": {"s3:ResourceAccount": config.account_id}},
                )
            )

        for role in (self.application_role, self.lambda_role):
            role.add_to_policy(
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.DENY,
                    actions=["s3:DeleteObject", "s3:DeleteObjectVersion", "s3:DeleteBucket"],
                    resources=[self.owner_arn, f"{self.owner_arn}/*"],
                )
            )

        outputs = {
            "VaultName": self.vault.backup_vault_name,
            "VaultArn": self.vault.backup_vault_arn,
            "BackupRoleArn": self.backup_role.role_arn,
            "ApplicationRoleArn": self.application_role.role_arn,
            "LambdaRoleArn": self.lambda_role.role_arn,
            "LifecycleRoleArn": self.lifecycle_role.role_arn,
            "AllowedOrgEnvironments": json.dumps({config.organization_id: ["prod"]}, sort_keys=True),
        }
        for name, value in outputs.items():
            cdk.CfnOutput(stack, f"CustomerStorage{name}", value=value)

    def _owner_grant(self, role: aws_iam.Role, actions: list[str], resources: list[str]) -> None:
        role.add_to_policy(
            aws_iam.PolicyStatement(actions=actions, resources=resources, conditions=self.account_condition)
        )

    def _read(self, role: aws_iam.Role, bucket_arn: str, account_id: str) -> None:
        condition = {"StringEquals": {"s3:ResourceAccount": account_id}}
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectVersion"],
                resources=[f"{bucket_arn}/{root}*" for root in _READ_ROOTS],
                conditions=condition,
            )
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:ListBucket", "s3:ListBucketVersions"],
                resources=[bucket_arn],
                conditions={**condition, "StringLike": {"s3:prefix": [f"{root}*" for root in _READ_ROOTS]}},
            )
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetBucketLocation"],
                resources=[bucket_arn],
                conditions=condition,
            )
        )

    def _application_permissions(self) -> None:
        role = self.application_role
        role.add_to_policy(aws_iam.PolicyStatement(actions=["s3:CreateBucket"], resources=[self.owner_arn]))
        self._owner_grant(
            role,
            [
                "s3:GetBucketVersioning",
                "s3:PutBucketVersioning",
                "s3:GetEncryptionConfiguration",
                "s3:PutEncryptionConfiguration",
                "s3:GetBucketPublicAccessBlock",
                "s3:PutBucketPublicAccessBlock",
                "s3:GetBucketOwnershipControls",
                "s3:PutBucketOwnershipControls",
                "s3:GetBucketPolicy",
                "s3:PutBucketPolicy",
                "s3:GetBucketTagging",
                "s3:PutBucketTagging",
                "s3:ListBucket",
                "s3:ListBucketVersions",
                "s3:ListBucketMultipartUploads",
            ],
            [self.owner_arn],
        )
        self._read(role, self.owner_arn, self.config.account_id)
        self._read(role, self.legacy_arn, self.config.legacy_account_id)
        self._owner_grant(
            role,
            ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
            [f"{self.owner_arn}/{root}*" for root in _APPLICATION_ROOTS],
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[
                    cdk.Stack.of(self).format_arn(
                        service="lambda",
                        resource="function",
                        resource_name=self.config.lambda_name,
                        arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
                    )
                ],
            )
        )

    def _lambda_permissions(self) -> None:
        self._read(self.lambda_role, self.owner_arn, self.config.account_id)
        self._read(self.lambda_role, self.legacy_arn, self.config.legacy_account_id)
        self._owner_grant(self.lambda_role, ["s3:PutObject"], [f"{self.owner_arn}/{root}*" for root in _VIEW_ROOTS])
        self.lambda_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                resources=[
                    cdk.Stack.of(self).format_arn(
                        service="logs",
                        resource="log-group",
                        resource_name=f"/aws/lambda/{self.config.lambda_name}:*",
                        arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
                    )
                ],
            )
        )

    def _backup_permissions(self, system_bucket: aws_s3.IBucket) -> None:
        role = self.backup_role
        buckets = [self.owner_arn, system_bucket.bucket_arn]
        self._owner_grant(
            role,
            [
                "s3:GetBucketTagging",
                "s3:ListTagsForResource",
                "s3:GetInventoryConfiguration",
                "s3:ListBucketVersions",
                "s3:ListBucket",
                "s3:GetBucketVersioning",
                "s3:GetBucketLocation",
                "s3:GetBucketAcl",
                "s3:PutInventoryConfiguration",
                "s3:GetBucketNotification",
                "s3:PutBucketNotification",
            ],
            buckets,
        )
        self._owner_grant(
            role,
            [
                "s3:GetObjectAcl",
                "s3:GetObject",
                "s3:GetObjectVersionTagging",
                "s3:GetObjectVersionAcl",
                "s3:GetObjectTagging",
                "s3:GetObjectVersion",
            ],
            [f"{bucket}/*" for bucket in buckets],
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["backup:TagResource"],
                resources=[self.recovery_point_arn],
                conditions={"StringEquals": {"aws:ResourceAccount": self.config.account_id}},
            )
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "events:DeleteRule",
                    "events:PutTargets",
                    "events:DescribeRule",
                    "events:EnableRule",
                    "events:PutRule",
                    "events:RemoveTargets",
                    "events:ListTargetsByRule",
                    "events:DisableRule",
                ],
                resources=[
                    cdk.Stack.of(self).format_arn(
                        service="events",
                        resource="rule",
                        resource_name="AwsBackupManagedRule*",
                        arn_format=cdk.ArnFormat.SLASH_RESOURCE_NAME,
                    )
                ],
            )
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["cloudwatch:GetMetricData", "events:ListRules", "s3:ListAllMyBuckets"],
                resources=["*"],
                conditions={"StringEquals": {"aws:RequestedRegion": cdk.Stack.of(self).region}},
            )
        )

    def _lifecycle_permissions(self) -> None:
        role = self.lifecycle_role
        self._owner_grant(
            role,
            [
                "s3:GetBucketLocation",
                "s3:GetBucketOwnershipControls",
                "s3:GetBucketTagging",
                "s3:GetBucketVersioning",
                "s3:GetBucketPolicy",
                "s3:PutBucketPolicy",
                "s3:ListBucket",
                "s3:ListBucketVersions",
                "s3:ListBucketMultipartUploads",
                "s3:DeleteBucket",
            ],
            [self.owner_arn],
        )
        self._owner_grant(
            role,
            [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:GetObjectTagging",
                "s3:GetObjectVersionTagging",
                "s3:PutObject",
                "s3:PutObjectTagging",
                "s3:PutObjectVersionTagging",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts",
            ],
            [f"{self.owner_arn}/*"],
        )
        self._read(role, self.legacy_arn, self.config.legacy_account_id)
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "backup:StartBackupJob",
                    "backup:DescribeBackupVault",
                    "backup:ListRecoveryPointsByBackupVault",
                    "backup:GetBackupVaultAccessPolicy",
                ],
                resources=[self.vault.backup_vault_arn],
            )
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["backup:DescribeRecoveryPoint", "backup:ListTags"],
                resources=[self.recovery_point_arn],
            )
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["backup:DeleteBackupAccessPoint", "backup:DescribeBackupAccessPoint"],
                resources=[self.access_point_arn],
            )
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetAccessPoint", "s3:DeleteAccessPoint"],
                resources=[
                    cdk.Stack.of(self).format_arn(
                        service="s3",
                        resource="accesspoint",
                        resource_name="*",
                        arn_format=cdk.ArnFormat.SLASH_RESOURCE_NAME,
                    )
                ],
            )
        )
        # These inventory/job APIs do not support resource-level permissions in the service authorization table.
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "s3:ListAllMyBuckets",
                    "backup:ListBackupJobs",
                    "backup:ListCopyJobs",
                    "backup:DescribeBackupJob",
                    "backup:DescribeCopyJob",
                    "backup:ListRecoveryPointsByResource",
                    "backup:ListBackupAccessPointsByRecoveryPoint",
                    "backup:ListBackupAccessPointsByResource",
                    "logs:DescribeLogGroups",
                ],
                resources=["*"],
                conditions={"StringEquals": {"aws:RequestedRegion": cdk.Stack.of(self).region}},
            )
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[self.backup_role.role_arn],
                conditions={"StringEquals": {"iam:PassedToService": "backup.amazonaws.com"}},
            )
        )
        role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "logs:DeleteLogGroup",
                    "logs:DescribeLogStreams",
                    "logs:GetLogEvents",
                    "logs:FilterLogEvents",
                    "logs:Unmask",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:PutRetentionPolicy",
                ],
                resources=[
                    cdk.Stack.of(self).format_arn(
                        service="logs",
                        resource="log-group",
                        resource_name="/valkyrie/benchmarks-prod/????????-????-????-????-????????????:*",
                        arn_format=cdk.ArnFormat.COLON_RESOURCE_NAME,
                    )
                ],
            )
        )
