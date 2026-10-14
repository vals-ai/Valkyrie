"""Give every hosted organization its own S3 bucket."""

import hashlib
from typing import TYPE_CHECKING, cast

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from sqlmodel import Session

from tracker import config
from tracker.aws.tags import ORG_ID_TAG
from tracker.database.models import Org

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.literals import BucketLocationConstraintType
    from mypy_boto3_s3.type_defs import CreateBucketConfigurationTypeDef


class TenantBucketError(RuntimeError):
    """The organization's bucket could not be named or created."""


def tenant_bucket_name(org_name: str) -> str:
    """Name an organization's bucket from a hash of its tenant name."""
    prefix = config.AWS_TENANT_BUCKET_PREFIX
    if not prefix:
        raise TenantBucketError("AWS_TENANT_BUCKET_PREFIX is not configured")
    return f"{prefix}-{hashlib.sha256(org_name.encode()).hexdigest()[:16]}"


def ensure_org_bucket(session: Session, org: Org) -> str:
    """Return the organization's bucket, creating and recording one the first time it is needed."""
    if org.storage_key:
        return org.storage_key

    bucket = tenant_bucket_name(org.name)
    _create_bucket(bucket, org)
    org.storage_key = bucket
    session.add(org)
    session.commit()
    session.refresh(org)
    return bucket


def ensure_managed_org_bucket(session: Session, org: Org) -> None:
    """Create the organization's bucket on a deployment with tenant buckets, if it has none yet."""
    if org.storage_key or not config.AWS_TENANT_BUCKET_PREFIX:
        return
    ensure_org_bucket(session, org)


def _ensure_org_tag(s3: "S3Client", bucket: str, org: Org, account_id: str) -> None:
    """Tag a bucket an earlier attempt left untagged; refuse one tagged for another organization.

    Runs after every create, because us-east-1 answers 200 when re-creating a bucket we already own.
    """
    try:
        tags = s3.get_bucket_tagging(Bucket=bucket, ExpectedBucketOwner=account_id)["TagSet"]
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") != "NoSuchTagSet":
            raise
        tags = []
    owner = next((tag["Value"] for tag in tags if tag["Key"] == ORG_ID_TAG), None)
    if owner == str(org.id):
        return
    if owner is not None:
        raise TenantBucketError(f"The organization bucket {bucket} belongs to another organization")
    s3control = boto3.client("s3control", region_name=s3.meta.region_name)  # pyright: ignore[reportUnknownMemberType]
    s3control.tag_resource(
        AccountId=account_id,
        ResourceArn=f"arn:aws:s3:::{bucket}",
        Tags=[{"Key": ORG_ID_TAG, "Value": str(org.id)}],
    )


def _create_bucket(bucket: str, org: Org) -> None:
    region = config.AWS_DEPLOYMENT_REGION
    account_id = config.AWS_DEPLOYMENT_ACCOUNT_ID
    if not region or not account_id:
        raise TenantBucketError("AWS_DEPLOYMENT_REGION and AWS_DEPLOYMENT_ACCOUNT_ID are required to create buckets")

    s3 = boto3.client("s3", region_name=region)  # pyright: ignore[reportUnknownMemberType]
    bucket_configuration: CreateBucketConfigurationTypeDef = {"Tags": [{"Key": ORG_ID_TAG, "Value": str(org.id)}]}
    if region != "us-east-1":
        bucket_configuration["LocationConstraint"] = cast("BucketLocationConstraintType", region)

    try:
        try:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration=bucket_configuration,
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") != "BucketAlreadyOwnedByYou":
                raise
        _ensure_org_tag(s3, bucket, org, account_id)

        # Match the deployment bucket: versioned, with old versions and abandoned uploads cleaned up.
        s3.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}, ExpectedBucketOwner=account_id
        )
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-noncurrent-versions-and-incomplete-uploads",
                        "Status": "Enabled",
                        "Filter": {},
                        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1},
                        "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
                    }
                ]
            },
            ExpectedBucketOwner=account_id,
        )
        s3.put_bucket_abac(Bucket=bucket, AbacStatus={"Status": "Enabled"}, ExpectedBucketOwner=account_id)
    except (ClientError, BotoCoreError) as error:
        raise TenantBucketError(f"Could not create the organization bucket {bucket}") from error
