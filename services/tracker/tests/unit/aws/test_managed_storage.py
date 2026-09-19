"""Managed owner-bucket policy and validation tests."""

from collections.abc import Mapping
from dataclasses import replace
from uuid import UUID

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from tracker import config
from tracker.aws.clients import DefaultChainAWSClientProvider
from tracker.aws.managed_storage import (
    ManagedStorageError,
    ManagedStoragePolicy,
    load_managed_storage_policy,
    validate_managed_storage_bucket,
    validate_managed_storage_bucket_versioning,
)
from tracker.aws.resolver import validate_saved_managed_storage_runtime
from tracker.aws.runtime import AWSResources, AWSRuntime

_ACCOUNT_ID = "123456789012"
_ORG_ID = UUID("00000000-0000-0000-0000-000000000001")
_OTHER_ORG_ID = UUID("00000000-0000-0000-0000-000000000002")


class RecordingS3Client:
    """Record bucket validation requests without calling AWS."""

    def __init__(
        self,
        *,
        region: str = "us-east-1",
        tags: Mapping[str, str] | None = None,
        head_error: BaseException | None = None,
        tag_error: BaseException | None = None,
        versioning_status: str | None = "Enabled",
    ) -> None:
        self.region = region
        self.tags = dict(
            tags
            if tags is not None
            else {
                "valsmith:environment": "dev",
                "valsmith:owner-account-id": "123",
                "valsmith:backup": "true",
                "valsmith:valkyrie-org-id": str(_ORG_ID),
            }
        )
        self.head_error = head_error
        self.tag_error = tag_error
        self.versioning_status = versioning_status
        self.head_requests: list[dict[str, str]] = []
        self.tag_requests: list[dict[str, str]] = []
        self.versioning_requests: list[dict[str, str]] = []

    async def __aenter__(self) -> "RecordingS3Client":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        pass

    async def head_bucket(self, **request: str) -> dict[str, str]:
        self.head_requests.append(request)
        if self.head_error is not None:
            raise self.head_error

        return {"BucketRegion": self.region}

    async def get_bucket_tagging(self, **request: str) -> dict[str, list[dict[str, str]]]:
        self.tag_requests.append(request)
        if self.tag_error is not None:
            raise self.tag_error

        return {"TagSet": [{"Key": key, "Value": value} for key, value in self.tags.items()]}

    async def get_bucket_versioning(self, **request: str) -> dict[str, str]:
        self.versioning_requests.append(request)
        return {"Status": self.versioning_status} if self.versioning_status is not None else {}


@pytest.fixture
def managed_runtime(monkeypatch: pytest.MonkeyPatch) -> AWSRuntime:
    runtime = AWSRuntime(
        resources=AWSResources(
            region="us-east-1",
            s3_bucket="shared-bucket",
            log_group="shared-logs",
            log_retention_days=30,
        ),
        clients=DefaultChainAWSClientProvider(region="us-east-1"),
        expected_bucket_owner=_ACCOUNT_ID,
    )
    client = RecordingS3Client()
    _inject_client(monkeypatch, client)

    return runtime


def _policy(
    *,
    environments: frozenset[str] = frozenset({"dev"}),
    org_id: UUID = _ORG_ID,
) -> ManagedStoragePolicy:
    return ManagedStoragePolicy(
        expected_account_id=_ACCOUNT_ID,
        org_environments={org_id: environments},
    )


def _client(runtime: AWSRuntime) -> RecordingS3Client:
    return runtime.clients.s3_client()


def _inject_client(monkeypatch: pytest.MonkeyPatch, client: RecordingS3Client) -> None:
    def s3_client(_provider: DefaultChainAWSClientProvider) -> RecordingS3Client:
        return client

    monkeypatch.setattr(DefaultChainAWSClientProvider, "s3_client", s3_client)


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "unsafe-provider-detail"}}, operation)


async def test_validator_checks_account_region_and_required_tags(managed_runtime: AWSRuntime) -> None:
    await validate_managed_storage_bucket(
        managed_runtime,
        org_id=_ORG_ID,
        bucket_name="vs-dev-acme-123",
        policy=_policy(),
    )

    client = _client(managed_runtime)
    expected_request = {"Bucket": "vs-dev-acme-123", "ExpectedBucketOwner": _ACCOUNT_ID}
    assert client.head_requests == [expected_request]
    assert client.tag_requests == [expected_request]


@pytest.mark.parametrize("versioning_status", [None, "Suspended"])
async def test_owner_bucket_admission_requires_enabled_versioning(
    monkeypatch: pytest.MonkeyPatch,
    managed_runtime: AWSRuntime,
    versioning_status: str | None,
) -> None:
    client = RecordingS3Client(versioning_status=versioning_status)
    _inject_client(monkeypatch, client)

    with pytest.raises(ManagedStorageError) as error:
        await validate_managed_storage_bucket_versioning(
            managed_runtime,
            bucket_name="vs-dev-acme-123",
        )

    assert error.value.status_code == 403
    assert client.versioning_requests == [{"Bucket": "vs-dev-acme-123", "ExpectedBucketOwner": _ACCOUNT_ID}]


@pytest.mark.parametrize(
    ("bucket_name", "policy", "status_code"),
    [
        pytest.param("s3://vs-dev-acme-123", _policy(), 400, id="uri"),
        pytest.param("vs-dev-Acme-123", _policy(), 400, id="uppercase"),
        pytest.param("vs-dev-acme--123", _policy(), 400, id="double-hyphen"),
        pytest.param("vs-dev-acme-", _policy(), 400, id="trailing-hyphen"),
        pytest.param("vs-dev-acme", _policy(), 400, id="missing-owner-id"),
        pytest.param("vs-dev-" + "a" * 40 + "-123", _policy(), 400, id="overlong-login"),
        pytest.param("vs-prod-acme-123", _policy(), 403, id="wrong-environment"),
        pytest.param("vs-dev-acme-123", _policy(org_id=_OTHER_ORG_ID), 403, id="unauthorized-org"),
        pytest.param("vs-dev-" + "a" * 57, _policy(), 400, id="64-characters"),
    ],
)
async def test_validator_rejects_invalid_or_unauthorized_names_before_aws_tags(
    managed_runtime: AWSRuntime,
    bucket_name: str,
    policy: ManagedStoragePolicy,
    status_code: int,
) -> None:
    client = _client(managed_runtime)

    with pytest.raises(ManagedStorageError) as error:
        await validate_managed_storage_bucket(
            managed_runtime,
            org_id=_ORG_ID,
            bucket_name=bucket_name,
            policy=policy,
        )

    assert error.value.status_code == status_code
    assert client.head_requests == []
    assert client.tag_requests == []


@pytest.mark.parametrize(
    ("tags", "expected_status"),
    [
        pytest.param({}, 403, id="missing-tags"),
        pytest.param(
            {
                "valsmith:environment": "dev",
                "valsmith:owner-account-id": "123",
                "valsmith:backup": "true",
                "valsmith:valkyrie-org-id": "foreign-sensitive-value",
            },
            403,
            id="wrong-org-tag",
        ),
        pytest.param(
            {
                "valsmith:environment": "dev",
                "valsmith:owner-account-id": "999",
                "valsmith:backup": "true",
                "valsmith:valkyrie-org-id": str(_ORG_ID),
            },
            403,
            id="wrong-owner-id-tag",
        ),
        pytest.param(
            {
                "valsmith:environment": "prod",
                "valsmith:owner-account-id": "123",
                "valsmith:backup": "true",
                "valsmith:valkyrie-org-id": str(_ORG_ID),
            },
            403,
            id="wrong-environment-tag",
        ),
        pytest.param(
            {
                "valsmith:environment": "dev",
                "valsmith:owner-account-id": "123",
                "valsmith:backup": "false",
                "valsmith:valkyrie-org-id": str(_ORG_ID),
            },
            403,
            id="backup-disabled",
        ),
    ],
)
async def test_validator_rejects_tag_mismatches_without_exposing_values(
    monkeypatch: pytest.MonkeyPatch,
    managed_runtime: AWSRuntime,
    tags: Mapping[str, str],
    expected_status: int,
) -> None:
    client = RecordingS3Client(tags=tags)
    _inject_client(monkeypatch, client)

    with pytest.raises(ManagedStorageError) as error:
        await validate_managed_storage_bucket(
            managed_runtime,
            org_id=_ORG_ID,
            bucket_name="vs-dev-acme-123",
            policy=_policy(),
        )

    assert error.value.status_code == expected_status
    assert "foreign-sensitive-value" not in str(error.value)
    assert client.head_requests
    assert client.tag_requests


@pytest.mark.parametrize(
    ("client", "expected_status"),
    [
        pytest.param(RecordingS3Client(head_error=_client_error("AccessDenied", "HeadBucket")), 403, id="foreign"),
        pytest.param(RecordingS3Client(region="us-west-2"), 403, id="wrong-region"),
        pytest.param(RecordingS3Client(head_error=_client_error("NoSuchBucket", "HeadBucket")), 403, id="missing"),
        pytest.param(RecordingS3Client(head_error=BotoCoreError()), 503, id="transient-core"),
        pytest.param(
            RecordingS3Client(tag_error=_client_error("SlowDown", "GetBucketTagging")),
            503,
            id="transient-client",
        ),
    ],
)
async def test_validator_classifies_aws_failures_with_safe_messages(
    monkeypatch: pytest.MonkeyPatch,
    managed_runtime: AWSRuntime,
    client: RecordingS3Client,
    expected_status: int,
) -> None:
    _inject_client(monkeypatch, client)

    with pytest.raises(ManagedStorageError) as error:
        await validate_managed_storage_bucket(
            managed_runtime,
            org_id=_ORG_ID,
            bucket_name="vs-dev-acme-123",
            policy=_policy(),
        )

    assert error.value.status_code == expected_status
    assert "unsafe-provider-detail" not in str(error.value)


@pytest.mark.parametrize(
    ("bucket_name", "owner_id"),
    [
        pytest.param("vs-dev-team-123-456", "456", id="numeric-login-component"),
        pytest.param("vs-dev-team-123-456-12345678", "456", id="numeric-collision-suffix"),
        pytest.param("vs-dev-" + "a" * 39 + "-1234567-abcdef12", "1234567", id="63-characters"),
    ],
)
async def test_validator_uses_owner_tag_to_parse_numeric_name_components(
    monkeypatch: pytest.MonkeyPatch,
    managed_runtime: AWSRuntime,
    bucket_name: str,
    owner_id: str,
) -> None:
    client = RecordingS3Client(
        tags={
            "valsmith:environment": "dev",
            "valsmith:owner-account-id": owner_id,
            "valsmith:backup": "true",
            "valsmith:valkyrie-org-id": str(_ORG_ID),
        }
    )
    _inject_client(monkeypatch, client)

    await validate_managed_storage_bucket(
        managed_runtime,
        org_id=_ORG_ID,
        bucket_name=bucket_name,
        policy=_policy(),
    )


@pytest.mark.parametrize(
    ("account_id", "mapping", "role_org_ids"),
    [
        pytest.param("123", "{}", str(_ORG_ID), id="invalid-account"),
        pytest.param(_ACCOUNT_ID, "[]", str(_ORG_ID), id="mapping-is-not-object"),
        pytest.param(_ACCOUNT_ID, "{", str(_ORG_ID), id="invalid-json"),
        pytest.param(_ACCOUNT_ID, f'{{"{_ORG_ID}": []}}', str(_ORG_ID), id="empty-environments"),
        pytest.param(_ACCOUNT_ID, f'{{"{_ORG_ID}": ["staging"]}}', str(_ORG_ID), id="unknown-environment"),
        pytest.param(_ACCOUNT_ID, f'{{"{_ORG_ID}": ["dev", "dev"]}}', str(_ORG_ID), id="duplicate-environment"),
        pytest.param(_ACCOUNT_ID, f'{{"{_ORG_ID}": ["dev"]}}', str(_OTHER_ORG_ID), id="org-outside-allowlist"),
    ],
)
def test_policy_loader_rejects_invalid_deployment_configuration(
    monkeypatch: pytest.MonkeyPatch,
    account_id: str,
    mapping: str,
    role_org_ids: str,
) -> None:
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ACCOUNT_ID", account_id)
    monkeypatch.setattr(config, "AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS", mapping)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", role_org_ids)

    with pytest.raises(ManagedStorageError) as error:
        load_managed_storage_policy()

    assert error.value.status_code == 500
    assert str(error.value) == "Managed storage configuration is invalid"


def test_policy_loader_parses_authorized_environments(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ACCOUNT_ID", _ACCOUNT_ID)
    monkeypatch.setattr(config, "AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS", f'{{"{_ORG_ID}": ["dev", "prod"]}}')
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", str(_ORG_ID))

    assert load_managed_storage_policy() == ManagedStoragePolicy(
        expected_account_id=_ACCOUNT_ID,
        org_environments={_ORG_ID: frozenset({"dev", "prod"})},
    )


@pytest.fixture
def owner_runtime(managed_runtime: AWSRuntime, monkeypatch: pytest.MonkeyPatch) -> AWSRuntime:
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ACCOUNT_ID", _ACCOUNT_ID)
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", str(_ORG_ID))
    monkeypatch.setattr(config, "AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS", f'{{"{_ORG_ID}": ["dev"]}}')

    return managed_runtime.with_resources(replace(managed_runtime.resources, s3_bucket="vs-dev-acme-123"))


def _freeze_clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Drive the revalidation cache from a list whose first item is the current time."""
    clock = [1000.0]
    monkeypatch.setattr("tracker.aws.resolver.monotonic", lambda: clock[0])

    return clock


async def test_saved_storage_reads_reuse_a_recent_bucket_validation(
    monkeypatch: pytest.MonkeyPatch,
    owner_runtime: AWSRuntime,
) -> None:
    monkeypatch.setattr(config, "AWS_MANAGED_STORAGE_VALIDATION_TTL_SECONDS", 300)
    clock = _freeze_clock(monkeypatch)
    client = _client(owner_runtime)

    for _ in range(5):
        await validate_saved_managed_storage_runtime(owner_runtime, org_id=_ORG_ID)

    assert len(client.head_requests) == 1
    assert len(client.tag_requests) == 1

    clock[0] += 301
    await validate_saved_managed_storage_runtime(owner_runtime, org_id=_ORG_ID)

    assert len(client.head_requests) == 2
    assert len(client.tag_requests) == 2


async def test_saved_storage_validation_is_not_shared_across_buckets_or_organizations(
    monkeypatch: pytest.MonkeyPatch,
    owner_runtime: AWSRuntime,
) -> None:
    monkeypatch.setattr(
        config, "AWS_MANAGED_STORAGE_ORG_ENVIRONMENTS", f'{{"{_ORG_ID}": ["dev"], "{_OTHER_ORG_ID}": ["dev"]}}'
    )
    monkeypatch.setattr(config, "AWS_DEPLOYMENT_ROLE_ORG_IDS", f"{_ORG_ID},{_OTHER_ORG_ID}")
    monkeypatch.setattr(config, "AWS_MANAGED_STORAGE_VALIDATION_TTL_SECONDS", 300)
    _ = _freeze_clock(monkeypatch)
    client = _client(owner_runtime)
    other_bucket = owner_runtime.with_resources(replace(owner_runtime.resources, s3_bucket="vs-dev-other-123"))

    await validate_saved_managed_storage_runtime(owner_runtime, org_id=_ORG_ID)

    with pytest.raises(ManagedStorageError) as error:
        await validate_saved_managed_storage_runtime(owner_runtime, org_id=_OTHER_ORG_ID)

    assert error.value.status_code == 403

    await validate_saved_managed_storage_runtime(other_bucket, org_id=_ORG_ID)

    assert [request["Bucket"] for request in client.head_requests] == [
        "vs-dev-acme-123",
        "vs-dev-acme-123",
        "vs-dev-other-123",
    ]


async def test_saved_storage_validation_never_caches_a_refusal(
    monkeypatch: pytest.MonkeyPatch,
    owner_runtime: AWSRuntime,
) -> None:
    monkeypatch.setattr(config, "AWS_MANAGED_STORAGE_VALIDATION_TTL_SECONDS", 300)
    _ = _freeze_clock(monkeypatch)
    client = RecordingS3Client(tag_error=_client_error("SlowDown", "GetBucketTagging"))
    _inject_client(monkeypatch, client)

    for _ in range(2):
        with pytest.raises(ManagedStorageError) as error:
            await validate_saved_managed_storage_runtime(owner_runtime, org_id=_ORG_ID)

        assert error.value.status_code == 503

    assert len(client.tag_requests) == 2


async def test_zero_ttl_revalidates_every_saved_storage_read(
    monkeypatch: pytest.MonkeyPatch,
    owner_runtime: AWSRuntime,
) -> None:
    monkeypatch.setattr(config, "AWS_MANAGED_STORAGE_VALIDATION_TTL_SECONDS", 0)
    _ = _freeze_clock(monkeypatch)
    client = _client(owner_runtime)

    for _ in range(3):
        await validate_saved_managed_storage_runtime(owner_runtime, org_id=_ORG_ID)

    assert len(client.head_requests) == 3


async def test_submission_validation_stays_uncached_for_the_read_path(
    monkeypatch: pytest.MonkeyPatch,
    owner_runtime: AWSRuntime,
) -> None:
    monkeypatch.setattr(config, "AWS_MANAGED_STORAGE_VALIDATION_TTL_SECONDS", 300)
    _ = _freeze_clock(monkeypatch)
    client = _client(owner_runtime)

    for _ in range(2):
        await validate_managed_storage_bucket(
            owner_runtime,
            org_id=_ORG_ID,
            bucket_name="vs-dev-acme-123",
            policy=_policy(),
        )

    assert len(client.head_requests) == 2

    await validate_saved_managed_storage_runtime(owner_runtime, org_id=_ORG_ID)

    assert len(client.head_requests) == 3
