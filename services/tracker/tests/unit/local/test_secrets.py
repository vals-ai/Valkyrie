"""Local credential scoping and literal file values.

Run: pytest tests/unit/local/test_secrets.py
"""

from pathlib import Path

import pytest

from tracker.exceptions import SecretsError
from tracker.local.secrets import InMemorySecretStore, load_execution_secrets
from tracker.runtime.secrets import resolve_secrets


class TestLocalSecrets:
    """Only declared keys enter the transient per-run store."""

    async def test_grouped_references_exclude_undeclared_values(self) -> None:
        references = {"FIRST_KEY": "shared", "SECOND_KEY": "shared", "THIRD_KEY": "other"}
        declared = {"FIRST_KEY": "first", "SECOND_KEY": "second", "THIRD_KEY": "third"}
        store = InMemorySecretStore(references, {**declared, "UNDECLARED": "private"})

        assert await resolve_secrets(references, store) == declared
        assert await store.get("shared") == {"FIRST_KEY": "first", "SECOND_KEY": "second"}
        assert await store.get("other") == {"THIRD_KEY": "third"}
        with pytest.raises(SecretsError, match="no values for secret reference 'UNDECLARED'"):
            await store.get("UNDECLARED")

    def test_missing_declared_keys_fail_before_store_is_available(self) -> None:
        with pytest.raises(SecretsError, match="missing keys: FIRST_KEY, SECOND_KEY"):
            InMemorySecretStore({"SECOND_KEY": "shared", "FIRST_KEY": "shared"}, {"UNDECLARED": "private"})

    def test_file_values_do_not_expand_host_credentials(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("HOST_ONLY_SECRET", "must-not-leak")
        source = tmp_path / "run.env"
        source.write_text("MODEL_KEY=${HOST_ONLY_SECRET}\nUNDECLARED=private\n", encoding="utf-8")

        values = load_execution_secrets(source, {"MODEL_KEY": "model-reference"})

        assert values == {"MODEL_KEY": "${HOST_ONLY_SECRET}"}

    def test_optional_and_missing_secret_files(self, tmp_path: Path) -> None:
        missing = tmp_path / "missing.env"

        assert load_execution_secrets(missing, {}) == {}
        assert load_execution_secrets(None, {"MODEL_KEY": "model-reference"}) == {}
        with pytest.raises(SecretsError, match="Configured local secrets file does not exist"):
            load_execution_secrets(missing, {"MODEL_KEY": "model-reference"})
