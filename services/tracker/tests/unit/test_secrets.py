from typing import Any

import pytest

import tracker.aws.secrets as secrets_module
from tracker.exceptions import SecretsError
from tracker.types import AWSCredentials


@pytest.fixture
def aws() -> AWSCredentials:
    return AWSCredentials(
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
        aws_default_region="us-east-1",
    )


@pytest.mark.parametrize("secret_bundles", [None, []])
def test_resolve_secrets_without_bundles_preserves_repeated_fetches(
    monkeypatch: pytest.MonkeyPatch,
    aws: AWSCredentials,
    secret_bundles: list[str] | None,
) -> None:
    fetches: list[str] = []

    def fetch_secret(secret_name: str, _aws: AWSCredentials) -> dict[str, str]:
        fetches.append(secret_name)
        return {"FIRST_KEY": "first", "SECOND_KEY": "second"}

    monkeypatch.setattr(secrets_module, "fetch_aws_secret", fetch_secret)

    resolved = secrets_module.resolve_secrets(
        {"FIRST_KEY": "shared-secret", "SECOND_KEY": "shared-secret"},
        aws,
        secret_bundles=secret_bundles,
    )

    assert resolved == {"FIRST_KEY": "first", "SECOND_KEY": "second"}
    assert fetches == ["shared-secret", "shared-secret"]


def test_resolve_secrets_without_bundles_preserves_failure_order(
    monkeypatch: pytest.MonkeyPatch,
    aws: AWSCredentials,
) -> None:
    fetches: list[str] = []

    def fetch_secret(secret_name: str, _aws: AWSCredentials) -> dict[str, str]:
        fetches.append(secret_name)
        if secret_name == "later-secret":
            raise AssertionError("later secret should not be fetched")
        return {"OTHER_KEY": "value"}

    monkeypatch.setattr(secrets_module, "fetch_aws_secret", fetch_secret)

    with pytest.raises(SecretsError, match="Key 'EXPECTED_KEY' not found"):
        secrets_module.resolve_secrets(
            {"EXPECTED_KEY": "first-secret", "LATER_KEY": "later-secret"},
            aws,
        )

    assert fetches == ["first-secret"]


def test_resolve_secrets_expands_bundles_with_precedence_and_fetches_once(
    monkeypatch: pytest.MonkeyPatch,
    aws: AWSCredentials,
) -> None:
    secret_values: dict[str, dict[str, Any] | str] = {
        "provider-bundle": {
            "OPENAI_API_KEY": "openai-key",
            "SHARED_KEY": "first-bundle-value",
        },
        "override-bundle": {
            "ANTHROPIC_API_KEY": "anthropic-key",
            "SHARED_KEY": "second-bundle-value",
        },
        "explicit-secret": {"SHARED_KEY": "explicit-value"},
        "standalone-secret": "standalone-value",
    }
    fetches: list[str] = []

    def fetch_secret(secret_name: str, _aws: AWSCredentials) -> dict[str, Any] | str:
        fetches.append(secret_name)
        return secret_values[secret_name]

    monkeypatch.setattr(secrets_module, "fetch_aws_secret", fetch_secret)

    resolved = secrets_module.resolve_secrets(
        {
            # Dual declaration is the mixed-version compatibility path.
            "OPENAI_API_KEY": "provider-bundle",
            "SHARED_KEY": "explicit-secret",
            "SERVICE_TOKEN": "standalone-secret",
        },
        aws,
        secret_bundles=["provider-bundle", "override-bundle"],
    )

    assert resolved == {
        "OPENAI_API_KEY": "openai-key",
        "ANTHROPIC_API_KEY": "anthropic-key",
        "SHARED_KEY": "explicit-value",
        "SERVICE_TOKEN": "standalone-value",
    }
    assert fetches == ["provider-bundle", "override-bundle", "explicit-secret", "standalone-secret"]


@pytest.mark.parametrize(
    ("bundle", "message", "sensitive_values"),
    [
        ("raw-secret-value", "must contain a JSON object", ("raw-secret-value",)),
        (
            {"INVALID-NAME": "secret-value"},
            "invalid environment variable name",
            ("INVALID-NAME", "secret-value"),
        ),
        ({"VALID_NAME": 123456789}, "must contain only string values", ("VALID_NAME", "123456789")),
    ],
)
def test_resolve_secrets_rejects_invalid_bundle_payloads_without_leaking_values(
    monkeypatch: pytest.MonkeyPatch,
    aws: AWSCredentials,
    bundle: Any,
    message: str,
    sensitive_values: tuple[str, ...],
) -> None:
    monkeypatch.setattr(secrets_module, "fetch_aws_secret", lambda *_args: bundle)

    with pytest.raises(SecretsError, match=message) as exc_info:
        secrets_module.resolve_secrets({}, aws, secret_bundles=["provider-bundle"])

    for sensitive_value in sensitive_values:
        assert sensitive_value not in str(exc_info.value)


def test_resolve_secrets_preserves_missing_explicit_json_key_error_with_bundle(
    monkeypatch: pytest.MonkeyPatch,
    aws: AWSCredentials,
) -> None:
    secret_values = {
        "provider-bundle": {"BUNDLE_KEY": "bundle-value"},
        "explicit-secret": {"OTHER_KEY": "value"},
    }
    monkeypatch.setattr(secrets_module, "fetch_aws_secret", lambda name, _aws: secret_values[name])

    with pytest.raises(SecretsError, match="Key 'EXPECTED_KEY' not found"):
        secrets_module.resolve_secrets(
            {"EXPECTED_KEY": "explicit-secret"},
            aws,
            secret_bundles=["provider-bundle"],
        )
