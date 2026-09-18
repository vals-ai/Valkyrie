"""Transient credentials use the shared reference resolver without persistence."""

import pytest

from tracker.exceptions import SecretsError
from tracker.local.secrets import InMemorySecretStore
from tracker.runtime.secrets import resolve_secrets


async def test_resolves_shared_references_and_releases_values() -> None:
    references = {"API_KEY": "agent", "TOKEN": "agent"}
    values = {"API_KEY": "first", "TOKEN": "second", "UNDECLARED": "ignored"}
    store = InMemorySecretStore(references, values)
    values["API_KEY"] = "changed"
    assert resolve_secrets(references, store) == {"API_KEY": "first", "TOKEN": "second"}
    assert await store.get_async("agent") == {"API_KEY": "first", "TOKEN": "second"}
    store.close()
    with pytest.raises(SecretsError, match="no values"):
        resolve_secrets(references, store)


def test_rejects_missing_keys_without_exposing_values() -> None:
    with pytest.raises(SecretsError) as error:
        InMemorySecretStore({"API_KEY": "agent", "TOKEN": "agent"}, {"API_KEY": "sensitive"})
    assert "sensitive" not in str(error.value)
