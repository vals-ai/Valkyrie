"""AWS Secrets Manager adapter tests."""

import asyncio
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


class FakeAsyncSecretsManagerClient:
    def __init__(self, response: dict[str, Any] | None = None, error: ClientError | None = None) -> None:
        self.response = response or {}
        self.error = error
        self.secret_ids: list[str] = []

    async def get_secret_value(self, *, SecretId: str) -> dict[str, Any]:
        self.secret_ids.append(SecretId)
        if self.error is not None:
            raise self.error
        return self.response


class FakeAsyncClientContext:
    def __init__(self, client: FakeAsyncSecretsManagerClient) -> None:
        self.client = client
        self.exited = False

    async def __aenter__(self) -> FakeAsyncSecretsManagerClient:
        return self.client

    async def __aexit__(self, *_args: object) -> None:
        self.exited = True


class FakeAsyncClientProvider:
    def __init__(self, context: FakeAsyncClientContext) -> None:
        self.context = context
        self.calls = 0

    def secretsmanager_async_client(self) -> FakeAsyncClientContext:
        self.calls += 1
        return self.context


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
async def test_get_preserves_json_and_raw_string_domains(secret_string: str, expected: object) -> None:
    client = FakeAsyncSecretsManagerClient({"SecretString": secret_string})
    context = FakeAsyncClientContext(client)
    provider = FakeAsyncClientProvider(context)

    assert await SecretsManagerStore(cast(AWSClientProvider, provider)).get("named-secret") == expected
    assert client.secret_ids == ["named-secret"]
    assert provider.calls == 1
    assert context.exited


@pytest.mark.parametrize(
    ("code", "message"),
    [
        ("ResourceNotFoundException", "Secret 'named-secret' does not exist in AWS Secrets Manager"),
        ("AccessDeniedException", "Access denied when retrieving secret 'named-secret'"),
        ("ThrottlingException", ""),
    ],
)
async def test_get_preserves_client_error_translation_and_cause(code: str, message: str) -> None:
    provider_error = ClientError({"Error": {"Code": code, "Message": "provider failure"}}, "GetSecretValue")
    client = FakeAsyncSecretsManagerClient(error=provider_error)

    with pytest.raises(SecretsError) as captured:
        await SecretsManagerStore(cast(AWSClientProvider, FakeAsyncClientProvider(FakeAsyncClientContext(client)))).get(
            "named-secret"
        )

    expected = message or f"Failed to retrieve secret 'named-secret': {provider_error}"
    assert str(captured.value) == f"Secret error: {expected}"
    assert captured.value.__cause__ is provider_error


async def test_get_cancels_underlying_request_and_exits_client() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class BlockingClient(FakeAsyncSecretsManagerClient):
        async def get_secret_value(self, *, SecretId: str) -> dict[str, Any]:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
            return {"SecretString": SecretId}

    context = FakeAsyncClientContext(BlockingClient())
    task = asyncio.create_task(
        SecretsManagerStore(cast(AWSClientProvider, FakeAsyncClientProvider(context))).get("named-secret")
    )
    await started.wait()

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()
    assert context.exited


def test_store_constructs_without_accessing_the_client() -> None:
    provider = FakeAsyncClientProvider(FakeAsyncClientContext(FakeAsyncSecretsManagerClient({"SecretString": "value"})))

    SecretsManagerStore(cast(AWSClientProvider, provider))

    assert provider.calls == 0
