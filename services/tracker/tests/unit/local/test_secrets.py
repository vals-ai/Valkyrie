"""Transient credentials use the shared reference resolver without persistence."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tracker.exceptions import SecretsError
from tracker.local.secrets import InMemorySecretStore
from tracker.runtime.secrets import resolve_secrets


async def test_resolves_shared_references_and_releases_values() -> None:
    references = {"API_KEY": "agent", "TOKEN": "agent"}
    values = {"API_KEY": "first", "TOKEN": "second"}
    store = InMemorySecretStore(references, values)
    values["API_KEY"] = "changed"
    assert resolve_secrets(references, store) == {"API_KEY": "first", "TOKEN": "second"}
    assert await store.get_async("agent") == {"API_KEY": "first", "TOKEN": "second"}
    store.close()
    with pytest.raises(SecretsError, match="no values"):
        resolve_secrets(references, store)


@pytest.mark.parametrize("values", [{}, {"API_KEY": "sensitive", "UNDECLARED": "sensitive"}])
def test_rejects_missing_or_undeclared_keys_without_exposing_values(values: dict[str, str]) -> None:
    with pytest.raises(SecretsError) as error:
        InMemorySecretStore({"API_KEY": "agent"}, values)
    assert "sensitive" not in str(error.value)


def test_installation_handoff_token_is_shared_and_private(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Concurrent hosts share one token without publishing a partial or world-readable file."""
    from tracker.local.secret_pipe import local_handoff_token

    monkeypatch.setenv("VALKYRIE_LOCAL_DATA_ROOT", str(tmp_path))
    with ThreadPoolExecutor(max_workers=8) as pool:
        pending = [pool.submit(local_handoff_token) for _ in range(8)]
        tokens = [future.result() for future in pending]
    assert len(set(tokens)) == 1
    token_path = tmp_path / ".execution-handoff-token"
    assert token_path.read_text() == tokens[0]
    assert token_path.stat().st_mode & 0o777 == 0o600
    assert list(tmp_path.iterdir()) == [token_path]
