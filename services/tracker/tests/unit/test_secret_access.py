from uuid import UUID

import pytest

from tracker import config
from tracker.runtime.secrets import secret_display_name
from tracker.secret_access import resolve_secret_name, validate_secret_names

_ORG_ID = UUID("00000000-0000-0000-0000-000000000001")
_OTHER_ORG_ID = UUID("00000000-0000-0000-0000-000000000002")


@pytest.fixture(autouse=True)
def shared_secret_tenants(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "internal.example, partner-*")


@pytest.mark.parametrize(
    ("org_name", "auth_required"),
    [("internal.example", True), ("partner-acme", True), ("acme.example", False)],
    ids=["listed-tenant", "listed-prefix", "self-hosted"],
)
def test_trusted_callers_may_use_shared_secrets(org_name: str, auth_required: bool) -> None:
    validate_secret_names(
        ["SharedDaytonaSecrets", "benchmark-services/shared-api-key"],
        org_id=_ORG_ID,
        org_name=org_name,
        auth_required=auth_required,
    )


def test_other_orgs_may_use_secrets_under_their_own_prefix() -> None:
    validate_secret_names(
        [f"valkyrie/tenants/{_ORG_ID}/daytona", f"valkyrie/tenants/{_ORG_ID}/model-keys"],
        org_id=_ORG_ID,
        org_name="acme.example",
        auth_required=True,
    )


@pytest.mark.parametrize(
    ("org_name", "secret_name"),
    [
        ("acme.example", "SharedDaytonaSecrets"),
        ("acme.example", f"valkyrie/tenants/{_OTHER_ORG_ID}/daytona"),
        ("acme.example", f"valkyrie/tenants/{_ORG_ID}-other/daytona"),
        ("acme.example", f"arn:aws:secretsmanager:us-east-1:123456789012:secret:valkyrie/tenants/{_ORG_ID}/daytona"),
        ("partneracme", "SharedDaytonaSecrets"),
        ("acme-partner-", "SharedDaytonaSecrets"),
        ("internal.example.evil", "SharedDaytonaSecrets"),
    ],
    ids=[
        "shared",
        "other-org",
        "lookalike-prefix",
        "arn",
        "prefix-without-dash",
        "prefix-not-at-start",
        "listed-name-as-prefix",
    ],
)
def test_other_orgs_cannot_name_secrets_outside_their_prefix(org_name: str, secret_name: str) -> None:
    with pytest.raises(ValueError) as error:
        validate_secret_names([secret_name], org_id=_ORG_ID, org_name=org_name, auth_required=True)

    shown = "daytona" if secret_name.startswith("valkyrie/tenants/") else secret_name
    assert str(error.value) == f"Secret '{shown}' does not exist"


def test_unset_list_limits_every_org_to_its_own_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "")

    with pytest.raises(ValueError):
        validate_secret_names(["SharedDaytonaSecrets"], org_id=_ORG_ID, org_name="internal.example", auth_required=True)


@pytest.mark.parametrize(
    ("secret_name", "shown"),
    [
        (f"valkyrie/tenants/{_ORG_ID}/daytona", "daytona"),
        (f"valkyrie/tenants/{_ORG_ID}/team/model-keys", "team/model-keys"),
        ("valkyrie/tenants/", "valkyrie/tenants/"),
        ("SharedDaytonaSecrets", "SharedDaytonaSecrets"),
        ("valkyrie/sentry-dsn", "valkyrie/sentry-dsn"),
    ],
    ids=["tenant", "nested", "bare-root", "shared", "platform"],
)
def test_tenant_secrets_are_shown_without_the_tenant_prefix(secret_name: str, shown: str) -> None:
    assert secret_display_name(secret_name) == shown


@pytest.mark.parametrize(
    ("org_name", "auth_required", "name", "resolved"),
    [
        ("acme.example", True, "daytona", f"valkyrie/tenants/{_ORG_ID}/daytona"),
        ("acme.example", True, f"valkyrie/tenants/{_ORG_ID}/daytona", f"valkyrie/tenants/{_ORG_ID}/daytona"),
        (
            "acme.example",
            True,
            f"valkyrie/tenants/{_OTHER_ORG_ID}/daytona",
            f"valkyrie/tenants/{_OTHER_ORG_ID}/daytona",
        ),
        ("internal.example", True, "SharedDaytonaSecrets", "SharedDaytonaSecrets"),
        ("acme.example", False, "SharedDaytonaSecrets", "SharedDaytonaSecrets"),
    ],
    ids=["short-name", "full-reference", "other-org-reference", "shared-tenant", "self-hosted"],
)
def test_customers_name_their_own_secrets_without_the_prefix(
    org_name: str, auth_required: bool, name: str, resolved: str
) -> None:
    assert resolve_secret_name(name, org_id=_ORG_ID, org_name=org_name, auth_required=auth_required) == resolved


@pytest.mark.parametrize("org_name", ["vals-smith-dev-gh-52346350", "vals-smith-prod-gh-52346350"])
@pytest.mark.parametrize(
    "shared_tenants", ["vals.ai,vals-smith-*", "vals-smith-dev-gh-52346350,vals-smith-prod-gh-52346350"]
)
def test_dedicated_customer_secret_names_stay_in_their_tenant(
    monkeypatch: pytest.MonkeyPatch, org_name: str, shared_tenants: str
) -> None:
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", shared_tenants)

    assert (
        resolve_secret_name("devModelGatewayClientConfig", org_id=_ORG_ID, org_name=org_name, auth_required=True)
        == f"valkyrie/tenants/{_ORG_ID}/devModelGatewayClientConfig"
    )

    with pytest.raises(ValueError):
        validate_secret_names(
            [f"valkyrie/tenants/{_OTHER_ORG_ID}/daytona"],
            org_id=_ORG_ID,
            org_name=org_name,
            auth_required=True,
        )


@pytest.mark.parametrize("org_name", ["vals.ai", "vals-smith-research", "vals-smith-dev-gh-research"])
def test_legacy_research_tenants_keep_explicit_shared_secret_access(
    monkeypatch: pytest.MonkeyPatch, org_name: str
) -> None:
    monkeypatch.setattr(config, "AWS_SHARED_SECRET_TENANTS", "vals.ai,vals-smith-*")

    assert (
        resolve_secret_name("SharedDaytonaSecrets", org_id=_ORG_ID, org_name=org_name, auth_required=True)
        == "SharedDaytonaSecrets"
    )
