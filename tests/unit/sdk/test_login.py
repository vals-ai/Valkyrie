from __future__ import annotations

import asyncio
import base64
import json
import stat
import threading
import time
from collections.abc import Callable
from concurrent.futures import Executor, ThreadPoolExecutor
from pathlib import Path
from typing import Any

import httpx
import pytest
from pydantic import BaseModel

from valkyrie.sdk import ValkyrieClient, ValkyrieConfig
from valkyrie.sdk import login as login_module
from valkyrie.sdk.config import DEFAULT_TRACKER_URL
from valkyrie.sdk.errors import ValkyrieConfigError
from valkyrie.sdk.login import LoginSession, SessionAuth, device_login

TRACKER = "https://tracker.test"
TOKEN_ENDPOINT = "https://login.test/oauth2/v1/token"
REFRESH_URL = "https://login.test/v1/auth/refresh"
SETTINGS = {
    "device_url": "https://login.test/oauth2/v1/device",
    "token_url": TOKEN_ENDPOINT,
    "client_id": "cli-client",
    "project_id": "P1",
    "refresh_url": REFRESH_URL,
}


def _session(expires_in: float = 3600, refresh_token: str | None = "refresh-1", **fields: Any) -> LoginSession:
    return LoginSession(
        tracker_url=TRACKER,
        project_id="P1",
        refresh_url=REFRESH_URL,
        access_token="access-1",
        refresh_token=refresh_token,
        expires_at=time.time() + expires_in,
        **fields,
    )


def _jwt(claims: dict[str, Any]) -> str:
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"header.{payload}.signature"


def test_sessions_are_saved_to_a_private_file(tmp_path: Path) -> None:
    _session(tenant="acme").save()

    (path,) = (tmp_path / "sessions").glob("*.json")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    loaded = LoginSession.load(TRACKER + "/")
    assert loaded is not None
    assert (loaded.access_token, loaded.refresh_token, loaded.tenant) == ("access-1", "refresh-1", "acme")
    assert LoginSession.forget(TRACKER) is True
    assert not path.exists()
    assert LoginSession.load(TRACKER) is None
    assert LoginSession.forget(TRACKER) is False


class TestCredentialPrecedence:
    def test_a_configured_key_then_the_environment_key_beat_a_saved_login(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _session().save()
        monkeypatch.setenv("VALKYRIE_API_KEY", "env-key")
        seen: list[httpx.Request] = []

        for api_key in ("config-key", None):
            auth = ValkyrieConfig.model_validate({"api_key": api_key, "url": TRACKER}).http_auth()
            with httpx.Client(transport=_tracker_and_login(seen, httpx.Response(500)), auth=auth) as client:
                client.get(f"{TRACKER}/auth/me")

        assert [request.headers["x-api-key"] for request in seen] == ["config-key", "env-key"]

    def test_the_saved_login_then_nothing(self) -> None:
        assert ValkyrieConfig(url=TRACKER).http_auth() is None

        _session().save()
        assert isinstance(ValkyrieConfig(url=TRACKER).http_auth(), SessionAuth)


@pytest.mark.parametrize(
    ("config", "environ", "tracker_url"),
    [
        ({"url": "https://given.test/"}, {"VALKYRIE_URL": "https://env.test"}, "https://given.test"),
        ({}, {"VALKYRIE_URL": "https://env.test/"}, "https://env.test"),
        ({}, {}, TRACKER),
        ({"api_key": "key"}, {}, DEFAULT_TRACKER_URL),
        ({}, {"VALKYRIE_API_KEY": "key"}, DEFAULT_TRACKER_URL),
    ],
)
def test_the_tracker_is_the_given_one_then_valkyrie_url_then_the_last_login(
    monkeypatch: pytest.MonkeyPatch, config: dict[str, str], environ: dict[str, str], tracker_url: str
) -> None:
    _session().save()
    _session().remember()
    for name, value in environ.items():
        monkeypatch.setenv(name, value)

    assert ValkyrieConfig.model_validate(config).url == tracker_url


def test_valkyrie_url_skips_an_unreadable_remembered_tracker(monkeypatch: pytest.MonkeyPatch) -> None:
    (login_module.DEFAULT_SESSION_DIR / "tracker").mkdir(parents=True)
    with pytest.raises(ValkyrieConfigError, match="Could not read the Tracker"):
        ValkyrieConfig()

    monkeypatch.setenv("VALKYRIE_URL", "https://env.test")
    assert ValkyrieConfig().url == "https://env.test"


def test_saved_lists_each_trackers_login_and_skips_unreadable_files() -> None:
    _session().save()
    _session().model_copy(update={"tracker_url": "https://self-hosted.test:8000"}).save()
    (login_module.DEFAULT_SESSION_DIR / "broken.json").write_text("{", encoding="utf-8")
    (login_module.DEFAULT_SESSION_DIR / "not-utf8.json").write_bytes(b'{"tracker_url": "\xff"}')

    assert sorted(login.tracker_url for login in LoginSession.saved()) == sorted(
        [TRACKER, "https://self-hosted.test:8000"]
    )


def _refreshed(name: str, refresh_token: str | None = None) -> tuple[httpx.Response, str]:
    session_jwt = _jwt({"exp": 1e10, "name": name})
    body: dict[str, Any] = {"sessionJwt": session_jwt}
    if refresh_token:
        body["refreshJwt"] = refresh_token
    return httpx.Response(200, json=body), session_jwt


def _tracker_and_login(seen: list[httpx.Request], token_response: httpx.Response) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if str(request.url) == REFRESH_URL:
            return token_response
        return httpx.Response(200, json={"ok": True})

    return httpx.MockTransport(handler)


class TestSessionAuth:
    def test_refresh_and_organization_save_share_a_lock(self, monkeypatch: pytest.MonkeyPatch) -> None:
        original = _session(expires_in=30, tenant="acme")
        original.save()
        auth = SessionAuth(original)
        refreshed, _ = _refreshed("access-2", "refresh-2")
        read = threading.Event()
        release_refresh = threading.Event()
        writer_started = threading.Event()
        writer_finished = threading.Event()
        load = LoginSession.load

        def paused_load(tracker_url: str) -> LoginSession | None:
            stored = load(tracker_url)
            read.set()
            assert release_refresh.wait(timeout=5)
            return stored

        def save_selection() -> None:
            writer_started.set()
            original.model_copy(update={"tenant": "beta"}).save()
            writer_finished.set()

        monkeypatch.setattr(LoginSession, "load", staticmethod(paused_load))
        with ThreadPoolExecutor(max_workers=2) as executor:
            refresh_future = executor.submit(auth._store_refresh, refreshed)
            assert read.wait(timeout=5)
            writer_future = executor.submit(save_selection)
            assert writer_started.wait(timeout=5)
            try:
                assert not writer_finished.wait(timeout=0.1)
            finally:
                release_refresh.set()
                refresh_future.result(timeout=5)
                writer_future.result(timeout=5)

        saved = load(TRACKER)
        assert saved is not None and saved.tenant == "beta"

    def test_refresh_preserves_a_new_saved_organization_and_the_clients_request_tenant(self) -> None:
        original = _session(expires_in=30, tenant="acme")
        original.save()
        auth = SessionAuth(original)
        refreshed, session_jwt = _refreshed("access-2", "refresh-2")
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if str(request.url) == REFRESH_URL:
                original.model_copy(update={"tenant": "beta"}).save()
                return refreshed

            return httpx.Response(200)

        with httpx.Client(transport=httpx.MockTransport(handler), auth=auth) as client:
            client.get(f"{TRACKER}/auth/me")

        saved = LoginSession.load(TRACKER)
        assert saved is not None
        assert (saved.tenant, saved.access_token, saved.refresh_token) == ("beta", session_jwt, "refresh-2")
        assert seen[-1].headers["x-valkyrie-tenant"] == "acme"

    @pytest.mark.parametrize("cancellations", [1, 2])
    async def test_cancelled_async_refresh_saves_rotated_tokens_before_releasing_authentication(
        self, monkeypatch: pytest.MonkeyPatch, cancellations: int
    ) -> None:
        original = _session(expires_in=30)
        original.save()
        refreshed, session_jwt = _refreshed("access-2", "refresh-2")
        seen: list[httpx.Request] = []
        release_worker = threading.Event()
        storage_submitted = asyncio.Event()
        loop = asyncio.get_running_loop()
        run_in_executor = loop.run_in_executor

        with ThreadPoolExecutor(max_workers=1) as worker_executor:
            occupied_worker = worker_executor.submit(release_worker.wait, 5)

            def queue_storage(
                executor: Executor | None, function: Callable[..., Any], *arguments: Any
            ) -> asyncio.Future[Any]:
                future = run_in_executor(executor or worker_executor, function, *arguments)
                storage_submitted.set()
                return future

            monkeypatch.setattr(loop, "run_in_executor", queue_storage)
            async with httpx.AsyncClient(
                transport=_tracker_and_login(seen, refreshed), auth=SessionAuth(original)
            ) as client:
                request = asyncio.create_task(client.get(f"{TRACKER}/auth/me"))
                try:
                    await asyncio.wait_for(storage_submitted.wait(), timeout=2)
                    for _ in range(cancellations):
                        request.cancel()
                        await asyncio.sleep(0)

                    assert not request.done(), "Cancellation must wait for rotated tokens to be saved"
                    release_worker.set()
                    with pytest.raises(asyncio.CancelledError):
                        await request

                    await loop.run_in_executor(worker_executor, lambda: None)
                    saved = LoginSession.load(TRACKER)
                    assert saved is not None
                    assert (saved.access_token, saved.refresh_token) == (session_jwt, "refresh-2")
                    await client.get(f"{TRACKER}/auth/me")
                finally:
                    release_worker.set()
                    request.cancel()
                    await asyncio.gather(request, return_exceptions=True)

            assert occupied_worker.result(timeout=2)

        assert [str(request.url) for request in seen] == [REFRESH_URL, f"{TRACKER}/auth/me"]
        assert seen[-1].headers["authorization"] == f"Bearer {session_jwt}"

    def test_refresh_does_not_replace_a_newer_saved_login(self) -> None:
        original = _session(expires_in=30, tenant="acme")
        original.save()
        replacement = _session(tenant="beta").model_copy(update={"access_token": "access-new-login"})
        auth = SessionAuth(original)
        refreshed, _ = _refreshed("access-2", "refresh-2")

        def handler(request: httpx.Request) -> httpx.Response:
            if str(request.url) == REFRESH_URL:
                replacement.save()
                return refreshed

            return httpx.Response(200)

        with httpx.Client(transport=httpx.MockTransport(handler), auth=auth) as client:
            client.get(f"{TRACKER}/auth/me")

        assert LoginSession.load(TRACKER) == replacement

    def test_requests_carry_the_session_and_tenant(self) -> None:
        seen: list[httpx.Request] = []
        auth = SessionAuth(_session(tenant="acme"))
        transport = _tracker_and_login(seen, httpx.Response(500))

        with httpx.Client(transport=transport, auth=auth) as client:
            client.get(f"{TRACKER}/auth/me")

        assert [request.headers["authorization"] for request in seen] == ["Bearer access-1"]
        assert seen[0].headers["x-valkyrie-tenant"] == "acme"

    def test_expiring_sessions_refresh_and_save_the_rotated_tokens(self) -> None:
        seen: list[httpx.Request] = []
        auth = SessionAuth(_session(expires_in=30, tenant="acme"))
        refreshed, session_jwt = _refreshed("access-2", "refresh-2")

        with httpx.Client(transport=_tracker_and_login(seen, refreshed), auth=auth, timeout=7) as client:
            client.get(f"{TRACKER}/auth/me")
            client.get(f"{TRACKER}/auth/me")

        refresh, first, second = seen
        assert (str(refresh.url), refresh.headers["authorization"]) == (REFRESH_URL, "Bearer P1:refresh-1")
        assert "x-valkyrie-tenant" not in refresh.headers
        assert refresh.extensions["timeout"]["read"] == 7
        assert json.loads(refresh.content) == {}
        assert first.headers["authorization"] == second.headers["authorization"] == f"Bearer {session_jwt}"
        saved = LoginSession.load(TRACKER)
        assert saved is not None and (saved.refresh_token, saved.expires_at) == ("refresh-2", 1e10)
        assert LoginSession.remembered_tracker() is None

    def test_a_session_another_process_refreshed_is_reused_for_the_same_organization(self) -> None:
        seen: list[httpx.Request] = []
        auth = SessionAuth(_session(expires_in=30, tenant="acme"))
        _session(tenant="beta").model_copy(update={"access_token": "access-elsewhere"}).save()

        with httpx.Client(transport=_tracker_and_login(seen, httpx.Response(500)), auth=auth) as client:
            client.get(f"{TRACKER}/auth/me")

        assert [str(request.url) for request in seen] == [f"{TRACKER}/auth/me"]
        assert seen[0].headers["authorization"] == "Bearer access-elsewhere"
        assert seen[0].headers["x-valkyrie-tenant"] == "acme"

    def test_another_accounts_saved_login_is_not_reused(self) -> None:
        mine = _jwt({"sub": "U-alice", "exp": time.time() + 30})
        auth = SessionAuth(_session().model_copy(update={"access_token": mine, "expires_at": time.time() + 30}))
        _session().model_copy(update={"access_token": _jwt({"sub": "U-bob", "exp": time.time() + 3600})}).save()
        seen: list[httpx.Request] = []

        with (
            httpx.Client(transport=_tracker_and_login(seen, httpx.Response(500)), auth=auth) as client,
            pytest.raises(ValkyrieConfigError),
        ):
            client.get(f"{TRACKER}/auth/me")

        assert [str(request.url) for request in seen] == [REFRESH_URL]

    def test_a_rotated_refresh_token_falls_back_to_the_stored_session(self) -> None:
        auth = SessionAuth(_session(expires_in=30))
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if str(request.url) == REFRESH_URL:
                _session().model_copy(update={"access_token": "access-elsewhere"}).save()
                return httpx.Response(400, json={"error": "invalid_grant"})
            return httpx.Response(200)

        with httpx.Client(transport=httpx.MockTransport(handler), auth=auth) as client:
            client.get(f"{TRACKER}/auth/me")

        assert seen[-1].headers["authorization"] == "Bearer access-elsewhere"

    async def test_async_clients_refresh_too(self) -> None:
        seen: list[httpx.Request] = []
        auth = SessionAuth(_session(expires_in=30))
        refreshed, session_jwt = _refreshed("access-2")

        async with httpx.AsyncClient(transport=_tracker_and_login(seen, refreshed), auth=auth) as client:
            await client.get(f"{TRACKER}/auth/me")

        assert seen[-1].headers["authorization"] == f"Bearer {session_jwt}"

    async def test_async_refresh_keeps_other_requests_running_while_login_is_locked(self) -> None:
        original = _session(expires_in=30, tenant="acme")
        original.save()
        refreshed, session_jwt = _refreshed("access-2", "refresh-2")
        lock_held = threading.Event()
        release_writer = threading.Event()
        refresh_received = asyncio.Event()
        seen: list[httpx.Request] = []

        def save_selection() -> bool:
            with original._lock(TRACKER):
                lock_held.set()
                loop_responded = release_writer.wait(timeout=2)
                original.model_copy(update={"tenant": "beta"})._write()

            return loop_responded

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            if str(request.url) == REFRESH_URL:
                refresh_received.set()
                return refreshed

            return httpx.Response(200)

        with ThreadPoolExecutor(max_workers=1) as executor:
            writer = executor.submit(save_selection)
            assert await asyncio.to_thread(lock_held.wait, 2)
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler), auth=SessionAuth(original)) as client:

                async def unrelated_request() -> None:
                    await refresh_received.wait()
                    response = await client.get(f"{TRACKER}/health", auth=None)
                    assert response.status_code == 200
                    release_writer.set()

                try:
                    await asyncio.gather(client.get(f"{TRACKER}/auth/me"), unrelated_request())
                finally:
                    release_writer.set()

            assert writer.result(timeout=2), "The login lock blocked the event loop"

        saved = LoginSession.load(TRACKER)
        assert saved is not None
        assert (saved.tenant, saved.access_token, saved.refresh_token) == ("beta", session_jwt, "refresh-2")
        assert seen[-1].headers["authorization"] == f"Bearer {session_jwt}"
        assert seen[-1].headers["x-valkyrie-tenant"] == "acme"

    @pytest.mark.parametrize(
        ("session", "response"),
        [
            (_session(expires_in=30), httpx.Response(400, json={"error": "invalid_grant"})),
            (_session(expires_in=30, refresh_token=None), httpx.Response(500)),
        ],
    )
    def test_a_login_that_cannot_refresh_asks_to_log_in_again(
        self, session: LoginSession, response: httpx.Response
    ) -> None:
        with httpx.Client(transport=_tracker_and_login([], response), auth=SessionAuth(session)) as client:
            with pytest.raises(ValkyrieConfigError, match="valkyrie login"):
                client.get(f"{TRACKER}/auth/me")


def _login_service(
    token_responses: list[httpx.Response] | None = None,
    *,
    settings: httpx.Response | None = None,
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == f"{TRACKER}/auth/cli":
            return settings or httpx.Response(200, json=SETTINGS)
        if url == "https://login.test/oauth2/v1/device":
            assert dict(httpx.QueryParams(request.content.decode())) == {"client_id": "cli-client", "scope": "openid"}
            return httpx.Response(
                200,
                json={
                    "device_code": "device-1",
                    "user_code": "ABCD-EFGH",
                    "verification_uri": "https://login.test/activate",
                    "verification_uri_complete": "https://login.test/activate?code=ABCD-EFGH",
                    "interval": 1,
                    "expires_in": 300,
                },
            )
        assert url == TOKEN_ENDPOINT
        assert token_responses
        return token_responses.pop(0)

    return httpx.MockTransport(handler)


def _pending(error: str) -> httpx.Response:
    return httpx.Response(400, json={"error": error})


class TestDeviceLogin:
    def test_login_waits_for_approval_and_backs_off_when_asked(self, monkeypatch: pytest.MonkeyPatch) -> None:
        waits: list[float] = []
        announced: list[tuple[str, str]] = []
        monkeypatch.setattr("valkyrie.sdk.login.time.sleep", waits.append)
        approved = httpx.Response(
            200, json={"access_token": "access-1", "refresh_token": "refresh-1", "expires_in": 600}
        )
        opaque = httpx.Response(200, json={"access_token": "opaque"})
        transport = _login_service([_pending("authorization_pending"), _pending("slow_down"), approved, opaque])

        with httpx.Client(transport=transport) as http:
            session = device_login(TRACKER + "/", http, lambda link, code: announced.append((link, code)))
            with pytest.raises(ValkyrieConfigError, match="without an expiry"):
                device_login(TRACKER, http, lambda _link, _code: None)

        assert announced == [("https://login.test/activate?code=ABCD-EFGH", "ABCD-EFGH")]
        assert waits == [1, 1, 6, 1]
        assert (session.tracker_url, session.project_id, session.refresh_url) == (TRACKER, "P1", REFRESH_URL)
        assert session.access_token == "access-1"
        assert session.expires_at == pytest.approx(time.time() + 600, abs=5)

    @pytest.mark.parametrize(
        ("response", "message"),
        [(_pending("access_denied"), "denied"), (_pending("expired_token"), "expired")],
    )
    def test_denied_or_expired_codes_stop_the_login(
        self, monkeypatch: pytest.MonkeyPatch, response: httpx.Response, message: str
    ) -> None:
        waits: list[float] = []
        monkeypatch.setattr("valkyrie.sdk.login.time.sleep", waits.append)
        with httpx.Client(transport=_login_service([response])) as http:
            with pytest.raises(ValkyrieConfigError, match=message):
                device_login(TRACKER, http, lambda _link, _code: None)

    @pytest.mark.parametrize(
        "settings",
        [
            httpx.Response(200, text="not json"),
            httpx.Response(200, json={**SETTINGS, "token_url": "http://login.test/oauth2/v1/token"}),
        ],
    )
    def test_malformed_or_insecure_login_services_are_refused(self, settings: httpx.Response) -> None:
        with httpx.Client(transport=_login_service(settings=settings)) as http:
            with pytest.raises(ValkyrieConfigError, match="malformed response"):
                device_login(TRACKER, http, lambda _link, _code: None)

    def test_trackers_without_cli_login_point_to_access_keys(self) -> None:
        with httpx.Client(transport=_login_service(settings=httpx.Response(404))) as http:
            with pytest.raises(ValkyrieConfigError, match="VALKYRIE_API_KEY"):
                device_login(TRACKER, http, lambda _link, _code: None)


class _Identity(BaseModel):
    organization: str


async def test_the_sdk_client_sends_the_saved_login(
    make_client: Callable[..., ValkyrieClient], sdk_config: Callable[..., ValkyrieConfig]
) -> None:
    _session().save()
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"organization": "acme"})

    async with make_client(handler, config=sdk_config(api_key=None)) as client:
        assert (await client.request_model("GET", "/auth/me", _Identity)).organization == "acme"

    assert seen[0].headers["authorization"] == "Bearer access-1"
    assert "x-api-key" not in seen[0].headers
