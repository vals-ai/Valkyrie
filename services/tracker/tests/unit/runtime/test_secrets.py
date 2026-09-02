"""Provider-neutral secret-reference resolution tests."""

from collections.abc import Mapping

import pytest

from tracker.exceptions import SecretsError
from tracker.runtime.secrets import (
    SecretValue,
    direct_provider_routing_forced,
    gateway_routing_enabled,
    resolve_secrets,
    without_direct_provider_credentials,
)


class RecordingSecretStore:
    def __init__(self, values: Mapping[str, SecretValue]) -> None:
        self._values = values
        self.calls: list[str] = []

    def get(self, name: str) -> SecretValue:
        self.calls.append(name)
        return self._values[name]


def test_resolve_secrets_skips_store_for_empty_mapping() -> None:
    store = RecordingSecretStore({})

    assert resolve_secrets({}, store) == {}
    assert store.calls == []


def test_resolve_secrets_requires_and_stringifies_object_members() -> None:
    store = RecordingSecretStore({"shared": {"TOKEN": "value", "COUNT": 7, "EMPTY": None}})

    assert resolve_secrets({"TOKEN": "shared", "COUNT": "shared", "EMPTY": "shared"}, store) == {
        "TOKEN": "value",
        "COUNT": "7",
        "EMPTY": "None",
    }
    assert store.calls == ["shared", "shared", "shared"]


def test_resolve_secrets_preserves_missing_object_key_error() -> None:
    store = RecordingSecretStore({"shared": {"OTHER": "value"}})

    with pytest.raises(SecretsError, match="Key 'TOKEN' not found"):
        resolve_secrets({"TOKEN": "shared"}, store)


@pytest.mark.parametrize("value", [["array"], 7, 3.5, True, None, "plain"])
def test_resolve_secrets_stringifies_non_object_values(value: SecretValue) -> None:
    store = RecordingSecretStore({"shared": value})

    assert resolve_secrets({"TOKEN": "shared"}, store)["TOKEN"] == str(value)


@pytest.mark.parametrize(
    ("secret_names", "kwargs", "expected"),
    [
        ({"MODEL_GATEWAY_URL", "MODEL_GATEWAY_API_KEY"}, {}, True),
        (
            {"MODEL_GATEWAY_URL", "MODEL_GATEWAY_API_KEY"},
            {"no_model_gateway": "False"},
            True,
        ),
        (
            {"MODEL_GATEWAY_URL", "MODEL_GATEWAY_API_KEY"},
            {"no_model_gateway": "True"},
            False,
        ),
        ({"MODEL_GATEWAY_URL"}, {}, False),
    ],
)
def test_gateway_routing_enabled(secret_names: set[str], kwargs: dict[str, str], expected: bool) -> None:
    assert gateway_routing_enabled(secret_names, kwargs) is expected


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({}, False),
        ({"no_model_gateway": "False"}, False),
        ({"no_model_gateway": "True"}, True),
    ],
)
def test_direct_provider_routing_forced(kwargs: dict[str, str], expected: bool) -> None:
    assert direct_provider_routing_forced(kwargs) is expected


def test_without_direct_provider_credentials_preserves_gateway_and_tool_keys() -> None:
    assert without_direct_provider_credentials(
        {
            "MODEL_GATEWAY_API_KEY": "gateway",
            "OPENAI_API_KEY": "provider",
            "GCP_CREDS": "provider-json",
            "AWS_ACCESS_KEY_ID": "bedrock-provider",
            "AWS_SECRET_ACCESS_KEY": "bedrock-provider",
            "AWS_SESSION_TOKEN": "bedrock-provider",
            "TAVILY_API_KEY": "tool",
        }
    ) == {
        "MODEL_GATEWAY_API_KEY": "gateway",
        "TAVILY_API_KEY": "tool",
    }
