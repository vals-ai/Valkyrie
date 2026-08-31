"""AWS Secrets Manager adapter tests."""

from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from tracker.aws.clients import AWSClientProvider
from tracker.aws.secrets import SecretsManagerStore
from tracker.exceptions import SecretsError


_ORIGINAL_GET = SecretsManagerStore.get


@pytest.fixture(autouse=True)
def use_real_secret_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SecretsManagerStore, "get", _ORIGINAL_GET)


class FakeSecretsManagerClient:
    def __init__(self, response: dict[str, Any] | None = None, error: ClientError | None = None) -> None:
        self.response = response or {}
        self.error = error
        self.secret_ids: list[str] = []

    def get_secret_value(self, *, SecretId: str) -> dict[str, Any]:
        self.secret_ids.append(SecretId)
        if self.error is not None:
            raise self.error
        return self.response


class FakeClientProvider:
    def __init__(self, client: FakeSecretsManagerClient) -> None:
        self.client = client
        self.calls = 0

    def secretsmanager_client(self) -> FakeSecretsManagerClient:
        self.calls += 1
        return self.client


@pytest.mark.parametrize(
    ("secret_string", "expected"),
    [
        ('{"key": "value"}', {"key": "value"}),
        ('["value", 1]', ["value", 1]),
        ('"json-string"', "json-string"),
        ("42", 42),
        ("true", True),
        ("null", None),
        ("plain-text", "plain-text"),
    ],
)
def test_get_preserves_json_and_raw_string_domains(secret_string: str, expected: object) -> None:
    client = FakeSecretsManagerClient({"SecretString": secret_string})
    provider = FakeClientProvider(client)

    assert SecretsManagerStore(cast(AWSClientProvider, provider)).get("named-secret") == expected
    assert client.secret_ids == ["named-secret"]
    assert provider.calls == 1


@pytest.mark.parametrize(
    ("code", "message"),
    [
        ("ResourceNotFoundException", "Secret 'named-secret' does not exist in AWS Secrets Manager"),
        ("AccessDeniedException", "Access denied when retrieving secret 'named-secret'"),
        ("ThrottlingException", ""),
    ],
)
def test_get_preserves_client_error_translation_and_cause(code: str, message: str) -> None:
    provider_error = ClientError({"Error": {"Code": code, "Message": "provider failure"}}, "GetSecretValue")
    client = FakeSecretsManagerClient(error=provider_error)

    with pytest.raises(SecretsError) as captured:
        SecretsManagerStore(cast(AWSClientProvider, FakeClientProvider(client))).get("named-secret")

    expected = message or f"Failed to retrieve secret 'named-secret': {provider_error}"
    assert str(captured.value) == f"Secret error: {expected}"
    assert captured.value.__cause__ is provider_error


def test_store_constructs_without_accessing_the_client() -> None:
    provider = FakeClientProvider(FakeSecretsManagerClient({"SecretString": "value"}))

    SecretsManagerStore(cast(AWSClientProvider, provider))

    assert provider.calls == 0
