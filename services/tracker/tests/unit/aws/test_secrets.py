"""AWS Secrets Manager adapter tests."""

import asyncio
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError
from botocore.session import get_session
from botocore.validate import ParamValidator

from tracker.aws.clients import AWSClientProvider
from tracker.aws.secrets import SecretBeingDeletedError, SecretsManagerStore
from tracker.exceptions import SecretsError


_ORIGINAL_GET = SecretsManagerStore.get


@pytest.fixture(autouse=True)
def use_real_secret_store(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SecretsManagerStore, "get", _ORIGINAL_GET)


_SECRETS_MANAGER = get_session().get_service_model("secretsmanager")


class FakeAsyncSecretsManagerClient:
    """Record calls; writes are checked against the Secrets Manager API model first."""

    def __init__(
        self,
        response: dict[str, Any] | None = None,
        error: ClientError | None = None,
        *,
        errors: dict[str, ClientError] | None = None,
        names: list[str] | None = None,
    ) -> None:
        self.response = response or {}
        self.error = error
        self.secret_ids: list[str] = []
        self.errors = errors or {}
        self.names = names or []
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def get_secret_value(self, *, SecretId: str) -> dict[str, Any]:
        self.secret_ids.append(SecretId)
        if self.error is not None:
            raise self.error
        return self.response

    def _call(self, operation: str, params: dict[str, Any]) -> dict[str, Any]:
        report = ParamValidator().validate(params, _SECRETS_MANAGER.operation_model(operation).input_shape)
        assert not report.has_errors(), report.generate_report()
        self.calls.append((operation, params))
        if operation in self.errors:
            raise self.errors[operation]
        return {}

    async def create_secret(self, **params: Any) -> dict[str, Any]:
        return self._call("CreateSecret", params)

    async def put_secret_value(self, **params: Any) -> dict[str, Any]:
        return self._call("PutSecretValue", params)

    async def delete_secret(self, **params: Any) -> dict[str, Any]:
        return self._call("DeleteSecret", params)

    def get_paginator(self, operation: str) -> "FakeAsyncSecretsManagerClient":
        assert operation == "list_secrets"
        return self

    def paginate(self, **params: Any) -> AsyncIterator[dict[str, Any]]:
        self._call("ListSecrets", params)
        return self._pages()

    async def _pages(self) -> AsyncIterator[dict[str, Any]]:
        for name in self.names:
            yield {"SecretList": [{"Name": name}]}


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
        ("ResourceNotFoundException", "Secret 'named-secret' does not exist"),
        ("AccessDeniedException", "Secret 'named-secret' does not exist"),
        ("ThrottlingException", "Failed to retrieve secret 'named-secret' (ThrottlingException)"),
    ],
)
async def test_get_preserves_client_error_translation_and_cause(code: str, message: str) -> None:
    provider_error = ClientError({"Error": {"Code": code, "Message": "provider failure"}}, "GetSecretValue")
    client = FakeAsyncSecretsManagerClient(error=provider_error)

    with pytest.raises(SecretsError) as captured:
        await SecretsManagerStore(cast(AWSClientProvider, FakeAsyncClientProvider(FakeAsyncClientContext(client)))).get(
            "named-secret"
        )

    assert str(captured.value) == f"Secret error: {message}"
    assert captured.value.__cause__ is provider_error


async def test_denied_tenant_secret_is_named_without_its_prefix_or_the_provider_error() -> None:
    provider_error = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "role/Tenant is not authorized on secret:valkyrie/tenants/x",
            }
        },
        "GetSecretValue",
    )
    client = FakeAsyncSecretsManagerClient(error=provider_error)

    with pytest.raises(SecretsError) as captured:
        await SecretsManagerStore(cast(AWSClientProvider, FakeAsyncClientProvider(FakeAsyncClientContext(client)))).get(
            "valkyrie/tenants/00000000-0000-0000-0000-000000000001/daytona"
        )

    assert str(captured.value) == "Secret error: Secret 'daytona' does not exist"


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


def _store(client: FakeAsyncSecretsManagerClient) -> SecretsManagerStore:
    context = FakeAsyncClientContext(client)
    return SecretsManagerStore(cast(AWSClientProvider, FakeAsyncClientProvider(context)))


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "provider failure"}}, operation)


_NAME = "valkyrie/tenants/org/daytona"
_CREATE = (
    "CreateSecret",
    {"Name": _NAME, "SecretString": "value", "Tags": [{"Key": "valkyrie:org-id", "Value": "org"}]},
)


@pytest.mark.parametrize(
    ("create_error", "calls"),
    [
        (None, [_CREATE]),
        ("ResourceExistsException", [_CREATE, ("PutSecretValue", {"SecretId": _NAME, "SecretString": "value"})]),
    ],
    ids=["new", "existing"],
)
async def test_put_creates_the_secret_or_replaces_its_value(create_error: str | None, calls: list[object]) -> None:
    client = FakeAsyncSecretsManagerClient(
        errors={"CreateSecret": _client_error(create_error, "CreateSecret")} if create_error else {}
    )

    await _store(client).put(_NAME, "value", tags={"valkyrie:org-id": "org"})

    assert client.calls == calls


@pytest.mark.parametrize("failing", ["CreateSecret", "PutSecretValue"])
async def test_put_reports_other_failures(failing: str) -> None:
    error = _client_error("AccessDeniedException", failing)
    errors = {failing: error}
    if failing == "PutSecretValue":
        errors["CreateSecret"] = _client_error("ResourceExistsException", "CreateSecret")

    with pytest.raises(SecretsError) as captured:
        await _store(FakeAsyncSecretsManagerClient(errors=errors)).put(_NAME, "value", tags={})

    assert str(captured.value) == "Secret error: Failed to save secret 'daytona' (AccessDeniedException)"
    assert captured.value.__cause__ is error


async def test_list_names_filters_by_prefix_and_sorts() -> None:
    client = FakeAsyncSecretsManagerClient(
        names=["valkyrie/tenants/org/modal", "valkyrie/tenants/org-other/x", "valkyrie/tenants/org/daytona"]
    )

    names = await _store(client).list_names("valkyrie/tenants/org/")

    assert names == ["valkyrie/tenants/org/daytona", "valkyrie/tenants/org/modal"]
    assert client.calls == [("ListSecrets", {"Filters": [{"Key": "name", "Values": ["valkyrie/tenants/org/"]}]})]


@pytest.mark.parametrize(
    ("error_code", "deleted"),
    [(None, True), ("AccessDeniedException", False)],
    ids=["deleted", "missing-under-tenant-role"],
)
async def test_delete_removes_the_secret_without_recovery(error_code: str | None, deleted: bool) -> None:
    errors = {"DeleteSecret": _client_error(error_code, "DeleteSecret")} if error_code else {}
    client = FakeAsyncSecretsManagerClient(errors=errors)

    assert await _store(client).delete(_NAME) is deleted
    assert client.calls == [("DeleteSecret", {"SecretId": _NAME, "ForceDeleteWithoutRecovery": True})]


async def test_put_reports_a_name_still_being_deleted() -> None:
    pending = ClientError(
        {
            "Error": {
                "Code": "InvalidRequestException",
                "Message": "a secret with this name is already scheduled for deletion",
            }
        },
        "CreateSecret",
    )

    with pytest.raises(SecretBeingDeletedError):
        await _store(FakeAsyncSecretsManagerClient(errors={"CreateSecret": pending})).put(_NAME, "value", tags={})
