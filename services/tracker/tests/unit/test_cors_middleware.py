"""CORS middleware behavior — hardcoded allowed origins + subdomain regex."""

import importlib

from fastapi.testclient import TestClient


def _client():
    from tracker import config as config_mod

    importlib.reload(config_mod)
    import main as main_mod

    importlib.reload(main_mod)
    return TestClient(main_mod.app)


def _preflight(client: TestClient, origin: str):
    return client.options(
        "/openapi.json",
        headers={"Origin": origin, "Access-Control-Request-Method": "GET"},
    )


def test_preflight_from_localhost_allowed():
    resp = _preflight(_client(), "http://localhost:3000")
    assert resp.headers.get("access-control-allow-origin") == "http://localhost:3000"
    assert resp.headers.get("access-control-allow-credentials") == "true"


def test_preflight_from_platform_subdomain_allowed():
    for origin in (
        "https://platform.vals.ai",
        "https://dev.platform.vals.ai",
        "https://bench.platform.vals.ai",
    ):
        resp = _preflight(_client(), origin)
        assert resp.headers.get("access-control-allow-origin") == origin, origin


def test_preflight_from_unrelated_origin_omits_header():
    resp = _preflight(_client(), "https://evil.example.com")
    assert resp.headers.get("access-control-allow-origin") != "https://evil.example.com"


def test_preflight_from_lookalike_origin_rejected():
    # Must not match a substring/lookalike of platform.vals.ai
    resp = _preflight(_client(), "https://platform.vals.ai.evil.com")
    assert resp.headers.get("access-control-allow-origin") != "https://platform.vals.ai.evil.com"
