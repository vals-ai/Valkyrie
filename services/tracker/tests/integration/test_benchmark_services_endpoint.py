from unittest.mock import AsyncMock


def test_benchmark_services_returns_pings(client, monkeypatch):
    async def fake_ping(_client, name: str, url: str):
        if name == "swebench":
            return {"name": "swebench", "url": url, "healthy": True, "latency_ms": 12, "error": None}
        return {"name": name, "url": url, "healthy": False, "latency_ms": None, "error": "timeout"}

    import tracker.api.benchmark_services as bs

    monkeypatch.setattr(bs, "_ping_service", AsyncMock(side_effect=fake_ping))

    resp = client.post(
        "/benchmark-services",
        headers={"Authorization": "Bearer fake"},
        json={
            "services": [
                {"name": "swebench", "url": "http://up:8001"},
                {"name": "fab", "url": "http://down:8002"},
            ]
        },
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    by_name = {s["name"]: s for s in data["services"]}
    assert by_name["swebench"]["healthy"] is True
    assert by_name["fab"]["healthy"] is False


def test_benchmark_services_unauth_401(client):
    resp = client.post("/benchmark-services", json={"services": []})
    assert resp.status_code == 401
