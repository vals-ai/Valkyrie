"""Provider-neutral secret-reference resolution tests."""

from collections.abc import Mapping

import pytest

from tracker.exceptions import SecretsError
from tracker.runtime.secrets import SecretValue, resolve_secrets


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
