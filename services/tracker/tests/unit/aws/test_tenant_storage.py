"""Behavioral tests for per-organization S3 buckets."""

import hashlib
from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from sqlmodel import Session

from tracker import config
from tracker.aws import tenant_storage
from tracker.aws.tenant_storage import (
    TenantBucketError,
    ensure_managed_org_bucket,
    ensure_org_bucket,
    tenant_bucket_name,
)
from tracker.database.models import Org

_PREFIX = "valkyrie-123456789012-dev"


@pytest.fixture(autouse=True)
def tenant_bucket_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "AWS_TENANT_BUCKET_PREFIX", _PREFIX)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_REGION", "us-east-1")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ACCOUNT_ID", "123456789012")


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    client = MagicMock()
    monkeypatch.setattr(tenant_storage.boto3, "client", MagicMock(return_value=client))
    return client


def _org(session: Session, name: str, storage_key: str | None = None) -> Org:
    org = Org(name=name, storage_key=storage_key)
    session.add(org)
    session.commit()
    session.refresh(org)
    return org


def test_bucket_name_hashes_the_tenant_name() -> None:
    """Bucket names never contain the tenant name and stay under S3's 63-character limit.

    Test cases:
    - The name is the configured prefix plus the first 16 hex characters of the tenant name's SHA-256.
    - Different tenants get different buckets.
    """
    name = tenant_bucket_name("acme.example")

    assert name == f"{_PREFIX}-{hashlib.sha256(b'acme.example').hexdigest()[:16]}"
    assert "acme" not in name
    assert len(name) <= 63
    assert tenant_bucket_name("other.example") != name


def test_bucket_name_requires_the_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "AWS_TENANT_BUCKET_PREFIX", None)

    with pytest.raises(TenantBucketError, match="AWS_TENANT_BUCKET_PREFIX"):
        tenant_bucket_name("acme.example")


def test_new_tenant_gets_a_configured_bucket_recorded_on_its_org(database_session: Session, s3: MagicMock) -> None:
    """A tenant's first bucket matches the deployment bucket's settings and is saved on the org row.

    Test cases:
    - The bucket is created tagged with the org id, versioned, given a lifecycle rule, and has ABAC turned on.
    - Every configuration call checks the expected bucket owner.
    - A second call returns the recorded bucket without calling S3 again.
    """
    org = _org(database_session, "acme.example")
    org_tag = {"Key": "valkyrie:org-id", "Value": str(org.id)}
    s3.get_bucket_tagging.return_value = {"TagSet": [org_tag]}

    bucket = ensure_org_bucket(database_session, org)

    assert bucket == tenant_bucket_name("acme.example")
    s3.create_bucket.assert_called_once_with(Bucket=bucket, CreateBucketConfiguration={"Tags": [org_tag]})
    owner = {"Bucket": bucket, "ExpectedBucketOwner": "123456789012"}
    s3.put_bucket_versioning.assert_called_once_with(VersioningConfiguration={"Status": "Enabled"}, **owner)
    rule = s3.put_bucket_lifecycle_configuration.call_args.kwargs["LifecycleConfiguration"]["Rules"][0]
    assert rule["AbortIncompleteMultipartUpload"] == {"DaysAfterInitiation": 1}
    assert rule["NoncurrentVersionExpiration"] == {"NoncurrentDays": 30}
    s3.put_bucket_abac.assert_called_once_with(AbacStatus={"Status": "Enabled"}, **owner)
    s3.tag_resource.assert_not_called()
    database_session.expire_all()
    stored = database_session.get(Org, org.id)
    assert stored is not None and stored.storage_key == bucket

    s3.reset_mock()
    assert ensure_org_bucket(database_session, org) == bucket
    s3.create_bucket.assert_not_called()


@pytest.mark.parametrize("region", ["us-east-1", "eu-west-1"])
def test_bucket_setup_requests_match_the_s3_api(
    database_session: Session, monkeypatch: pytest.MonkeyPatch, region: str
) -> None:
    """Every setup call is valid for the installed S3 SDK, which a mock client never checks."""
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_REGION", region)
    client = boto3.client("s3", region_name=region, aws_access_key_id="test", aws_secret_access_key="test")  # pyright: ignore[reportUnknownMemberType]
    monkeypatch.setattr(tenant_storage.boto3, "client", MagicMock(return_value=client))
    org = _org(database_session, "acme.example")
    bucket = tenant_bucket_name("acme.example")

    with Stubber(client) as stubber:
        stubber.add_response(
            "create_bucket",
            {},
            {
                "Bucket": bucket,
                "CreateBucketConfiguration": {
                    "Tags": [{"Key": "valkyrie:org-id", "Value": str(org.id)}],
                    **({"LocationConstraint": region} if region != "us-east-1" else {}),
                },
            },
        )
        stubber.add_response(
            "get_bucket_tagging",
            {"TagSet": [{"Key": "valkyrie:org-id", "Value": str(org.id)}]},
            {"Bucket": bucket, "ExpectedBucketOwner": "123456789012"},
        )
        stubber.add_response("put_bucket_versioning", {})
        stubber.add_response("put_bucket_lifecycle_configuration", {})
        stubber.add_response(
            "put_bucket_abac",
            {},
            {"Bucket": bucket, "AbacStatus": {"Status": "Enabled"}, "ExpectedBucketOwner": "123456789012"},
        )

        assert ensure_org_bucket(database_session, org) == bucket
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize(
    ("create_error", "untagged"),
    [("BucketAlreadyOwnedByYou", True), (None, True), ("BucketAlreadyOwnedByYou", False)],
    ids=["owned-untagged", "us-east-1-recreate-untagged", "owned-tagged"],
)
def test_an_existing_bucket_is_tagged_for_its_org(
    database_session: Session, s3: MagicMock, create_error: str | None, untagged: bool
) -> None:
    if create_error:
        s3.create_bucket.side_effect = ClientError({"Error": {"Code": create_error}}, "CreateBucket")
    org = _org(database_session, "acme.example")
    if untagged:
        s3.get_bucket_tagging.side_effect = ClientError({"Error": {"Code": "NoSuchTagSet"}}, "GetBucketTagging")
    else:
        s3.get_bucket_tagging.return_value = {"TagSet": [{"Key": "valkyrie:org-id", "Value": str(org.id)}]}

    bucket = ensure_org_bucket(database_session, org)

    assert bucket == tenant_bucket_name("acme.example")
    s3.put_bucket_abac.assert_called_once()
    s3.put_bucket_tagging.assert_not_called()
    if untagged:
        s3.tag_resource.assert_called_once_with(
            AccountId="123456789012",
            ResourceArn=f"arn:aws:s3:::{bucket}",
            Tags=[{"Key": "valkyrie:org-id", "Value": str(org.id)}],
        )
    else:
        s3.tag_resource.assert_not_called()


def test_bucket_tagged_for_another_org_is_refused(database_session: Session, s3: MagicMock) -> None:
    s3.create_bucket.side_effect = ClientError({"Error": {"Code": "BucketAlreadyOwnedByYou"}}, "CreateBucket")
    s3.get_bucket_tagging.return_value = {"TagSet": [{"Key": "valkyrie:org-id", "Value": "another-org"}]}
    org = _org(database_session, "acme.example")

    with pytest.raises(TenantBucketError, match="another organization"):
        ensure_org_bucket(database_session, org)

    s3.tag_resource.assert_not_called()
    s3.put_bucket_abac.assert_not_called()
    database_session.expire_all()
    stored = database_session.get(Org, org.id)
    assert stored is not None and stored.storage_key is None


@pytest.mark.parametrize(
    "failing_call", ["create_bucket", "get_bucket_tagging", "tag_resource", "put_bucket_versioning", "put_bucket_abac"]
)
def test_failed_bucket_setup_leaves_the_org_without_a_bucket(
    database_session: Session, s3: MagicMock, failing_call: str
) -> None:
    error: Any = ClientError({"Error": {"Code": "AccessDenied"}}, failing_call)
    getattr(s3, failing_call).side_effect = error
    org = _org(database_session, "acme.example")

    with pytest.raises(TenantBucketError):
        ensure_org_bucket(database_session, org)

    database_session.expire_all()
    stored = database_session.get(Org, org.id)
    assert stored is not None and stored.storage_key is None


def test_recorded_storage_key_is_used_as_is(database_session: Session, s3: MagicMock) -> None:
    """Vals' migrated row points at the deployment bucket and follows the same path as every org."""
    org = _org(database_session, "vals.ai", storage_key="agentic-harness")

    assert ensure_org_bucket(database_session, org) == "agentic-harness"
    s3.create_bucket.assert_not_called()


@pytest.mark.parametrize(
    ("prefix", "eligible"),
    [(None, True), (_PREFIX, False)],
    ids=["no-prefix", "not-eligible"],
)
def test_managed_bucket_is_only_created_for_eligible_orgs_in_hosted_deployments(
    database_session: Session,
    s3: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    prefix: str | None,
    eligible: bool,
) -> None:
    org = _org(database_session, "acme.example")
    monkeypatch.setattr(config, "AWS_TENANT_BUCKET_PREFIX", prefix)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", str(org.id) if eligible else "")

    ensure_managed_org_bucket(database_session, org)

    s3.create_bucket.assert_not_called()
    assert org.storage_key is None


def test_managed_bucket_is_created_for_an_eligible_org(
    database_session: Session, s3: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    org = _org(database_session, "acme.example")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", str(org.id))

    ensure_managed_org_bucket(database_session, org)

    assert org.storage_key == tenant_bucket_name("acme.example")


def test_managed_bucket_is_created_for_a_shared_secret_tenant(
    database_session: Session, s3: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    org = _org(database_session, "partner-acme")
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", "")
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "partner-*")

    ensure_managed_org_bucket(database_session, org)

    assert org.storage_key == tenant_bucket_name("partner-acme")
