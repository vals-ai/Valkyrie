from pathlib import Path

import pytest

import valkyrie.sdk.login as login_module


@pytest.fixture(autouse=True)
def isolated_login(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(login_module, "DEFAULT_SESSION_DIR", tmp_path / "sessions")
    monkeypatch.setattr("dotenv.main.find_dotenv", lambda *_args, **_kwargs: "")
    for name in ("VALKYRIE_API_KEY", "VALKYRIE_URL"):
        monkeypatch.delenv(name, raising=False)
