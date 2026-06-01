import importlib
import os
from unittest.mock import patch

import pytest


def test_cors_origins_parsed_from_env():
    with patch.dict(os.environ, {"CORS_ALLOWED_ORIGINS": "http://localhost:5173,https://example.com"}):
        from tracker import config

        importlib.reload(config)
        assert config.CORS_ALLOWED_ORIGINS == ["http://localhost:5173", "https://example.com"]


def test_cors_origins_empty_when_unset():
    env_without_cors = {k: v for k, v in os.environ.items() if k != "CORS_ALLOWED_ORIGINS"}
    with patch.dict(os.environ, env_without_cors, clear=True):
        with patch("dotenv.load_dotenv"):
            from tracker import config

            importlib.reload(config)
            assert config.CORS_ALLOWED_ORIGINS == []


def test_cors_origins_rejects_wildcard():
    with patch.dict(os.environ, {"CORS_ALLOWED_ORIGINS": "*"}):
        from tracker import config

        with pytest.raises(ValueError, match="wildcard"):
            importlib.reload(config)


def test_cors_preflight_from_allowed_origin():
    from fastapi.testclient import TestClient

    with patch.dict(os.environ, {"CORS_ALLOWED_ORIGINS": "http://localhost:5173"}):
        from tracker import config as config_mod

        importlib.reload(config_mod)
        import main as main_mod

        importlib.reload(main_mod)
        client = TestClient(main_mod.app)
        resp = client.options(
            "/openapi.json",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert resp.headers.get("access-control-allow-origin") == "http://localhost:5173"
        assert resp.headers.get("access-control-allow-credentials") == "true"


def test_cors_preflight_from_disallowed_origin_omits_header():
    from fastapi.testclient import TestClient

    with patch.dict(os.environ, {"CORS_ALLOWED_ORIGINS": "http://localhost:5173"}):
        from tracker import config as config_mod

        importlib.reload(config_mod)
        import main as main_mod

        importlib.reload(main_mod)
        client = TestClient(main_mod.app)
        resp = client.options(
            "/openapi.json",
            headers={
                "Origin": "https://evil.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert resp.headers.get("access-control-allow-origin") != "https://evil.example.com"
