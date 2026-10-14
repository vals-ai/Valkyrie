from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import os
import tempfile
import time
from collections.abc import AsyncGenerator, Callable, Generator
from pathlib import Path
from typing import Annotated, Any, cast

import httpx
from filelock import FileLock
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, ValidationError

from valkyrie.sdk.errors import ValkyrieConfigError

DEFAULT_SESSION_DIR = Path("~/.config/valkyrie/sessions")
REFRESH_MARGIN_SECONDS = 60.0
_LOGIN_EXPIRED = "Your Valkyrie login has expired. Run `valkyrie login` again."


def _claims(access_token: str) -> dict[str, Any]:
    try:
        segment = access_token.split(".")[1]
        claims: object = json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))
    except (IndexError, ValueError, binascii.Error):
        return {}
    return cast(dict[str, Any], claims) if isinstance(claims, dict) else {}


def _expiry(access_token: str, expires_in: float | None = None) -> float:
    if expires_in is not None:
        return time.time() + expires_in
    expires_at = _claims(access_token).get("exp")
    if not isinstance(expires_at, int | float):
        raise ValkyrieConfigError("The login service returned a token without an expiry")
    return float(expires_at)


def _secure(url: str) -> str:
    parsed = httpx.URL(url)
    if not (parsed.scheme == "https" or (parsed.scheme == "http" and parsed.host in {"localhost", "127.0.0.1", "::1"})):
        raise ValueError("insecure URL")
    return url


SecureUrl = Annotated[str, AfterValidator(_secure)]


class _Response(BaseModel):
    model_config = ConfigDict(hide_input_in_errors=True)


class _Settings(_Response):
    client_id: str
    project_id: str
    device_url: SecureUrl
    token_url: SecureUrl
    refresh_url: SecureUrl


class _Device(_Response):
    device_code: str = Field(repr=False)
    user_code: str
    verification_uri: SecureUrl
    verification_uri_complete: SecureUrl | None = None
    interval: float = 5
    expires_in: float = 600


class _Tokens(_Response):
    access_token: str = Field(repr=False)
    refresh_token: str | None = Field(default=None, repr=False)
    expires_in: float | None = None


class _Refreshed(_Response):
    sessionJwt: str = Field(repr=False)
    refreshJwt: str | None = Field(default=None, repr=False)


def _replace(path: Path, text: str) -> None:
    """Write a file in the 0700 sessions directory so readers never see it half-written."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete_on_close=False) as file:
        file.write(text)
        file.close()
        os.replace(file.name, path)


def _read[Model: _Response](model: type[Model], response: httpx.Response, action: str) -> Model:
    if response.is_error:
        raise ValkyrieConfigError(f"{action} failed ({response.status_code})")
    try:
        return model.model_validate_json(response.content)
    except ValidationError as error:
        raise ValkyrieConfigError(f"{action} returned a malformed response") from error


class LoginSession(BaseModel):
    model_config = ConfigDict(frozen=True, hide_input_in_errors=True)

    tracker_url: str
    project_id: str
    refresh_url: str
    access_token: str = Field(repr=False)
    refresh_token: str | None = Field(default=None, repr=False)
    expires_at: float
    tenant: str | None = None

    @property
    def expiring(self) -> bool:
        return self.expires_at - time.time() <= REFRESH_MARGIN_SECONDS

    @staticmethod
    def _path(tracker_url: str) -> Path:
        digest = hashlib.sha256(tracker_url.rstrip("/").encode()).hexdigest()[:24]
        return DEFAULT_SESSION_DIR.expanduser() / f"{digest}.json"

    @classmethod
    def load(cls, tracker_url: str) -> LoginSession | None:
        try:
            return cls.model_validate_json(cls._path(tracker_url).read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except OSError as error:
            raise ValkyrieConfigError(f"Could not read the saved Valkyrie login: {error}") from error
        except ValidationError as error:
            raise ValkyrieConfigError("The saved Valkyrie login is unreadable. Run `valkyrie login` again.") from error

    @classmethod
    def _lock(cls, tracker_url: str) -> FileLock:
        path = cls._path(tracker_url)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        return FileLock(path.with_suffix(".lock"), mode=0o600)

    def save(self) -> None:
        with self._lock(self.tracker_url):
            self._write()

    def save_refreshed(self, previous: LoginSession) -> None:
        """Update tokens without replacing a newer saved login or organization selection."""
        with self._lock(self.tracker_url):
            stored = self.load(self.tracker_url)
            if stored is None:
                self._write()
            elif stored.access_token == previous.access_token and stored.refresh_token == previous.refresh_token:
                self.model_copy(update={"tenant": stored.tenant})._write()

    def _write(self) -> None:
        _replace(self._path(self.tracker_url), self.model_dump_json())

    @staticmethod
    def _remembered_path() -> Path:
        return DEFAULT_SESSION_DIR.expanduser() / "tracker"

    @classmethod
    def saved(cls) -> list[LoginSession]:
        """Return every saved login, one per Tracker, skipping files that can't be read."""
        logins: list[LoginSession] = []
        for path in sorted(DEFAULT_SESSION_DIR.expanduser().glob("*.json")):
            try:
                logins.append(cls.model_validate_json(path.read_bytes()))
            except (OSError, ValidationError):
                continue
        return logins

    @classmethod
    def remembered_tracker(cls) -> str | None:
        """The Tracker the last `valkyrie login` signed in to."""
        try:
            return cls._remembered_path().read_text(encoding="utf-8").strip() or None
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as error:
            raise ValkyrieConfigError(f"Could not read the Tracker saved by `valkyrie login`: {error}") from error

    def remember(self) -> None:
        _replace(self._remembered_path(), self.tracker_url)

    @classmethod
    def forget(cls, tracker_url: str) -> bool:
        if cls.remembered_tracker() == tracker_url.rstrip("/"):
            cls._remembered_path().unlink(missing_ok=True)
        with cls._lock(tracker_url):
            try:
                cls._path(tracker_url).unlink()
            except FileNotFoundError:
                return False
        return True


class ApiKeyAuth(httpx.Auth):
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        request.headers["X-Api-Key"] = self._api_key
        yield request


class SessionAuth(httpx.Auth):
    def __init__(self, session: LoginSession) -> None:
        self._session = session
        self._lock = asyncio.Lock()

    def _refresh_request(self, extensions: dict[str, Any]) -> httpx.Request:
        if self._session.refresh_token is None:
            raise ValkyrieConfigError(_LOGIN_EXPIRED)
        return httpx.Request(
            "POST",
            self._session.refresh_url,
            headers={"Authorization": f"Bearer {self._session.project_id}:{self._session.refresh_token}"},
            json={},
            extensions=extensions,
        )

    def _adopt_stored(self) -> bool:
        stored = LoginSession.load(self._session.tracker_url)
        if (
            stored is None
            or stored.access_token == self._session.access_token
            or stored.expiring
            or _claims(stored.access_token).get("sub") != _claims(self._session.access_token).get("sub")
        ):
            return False
        self._session = stored.model_copy(update={"tenant": self._session.tenant})
        return True

    def _store_refresh(self, response: httpx.Response) -> None:
        if response.status_code in (400, 401):
            if self._adopt_stored():
                return
            raise ValkyrieConfigError(_LOGIN_EXPIRED)
        refreshed = _read(_Refreshed, response, "Refreshing the Valkyrie login")
        previous = self._session
        self._session = self._session.model_copy(
            update={
                "access_token": refreshed.sessionJwt,
                "refresh_token": refreshed.refreshJwt or self._session.refresh_token,
                "expires_at": _expiry(refreshed.sessionJwt),
            }
        )
        self._session.save_refreshed(previous)

    def _authorize(self, request: httpx.Request) -> httpx.Request:
        request.headers["Authorization"] = f"Bearer {self._session.access_token}"
        if self._session.tenant:
            request.headers["X-Valkyrie-Tenant"] = self._session.tenant
        return request

    def sync_auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        if self._session.expiring and not self._adopt_stored():
            response = yield self._refresh_request(request.extensions)
            response.read()
            self._store_refresh(response)
        yield self._authorize(request)

    async def async_auth_flow(self, request: httpx.Request) -> AsyncGenerator[httpx.Request, httpx.Response]:
        async with self._lock:
            if self._session.expiring and not self._adopt_stored():
                response = yield self._refresh_request(request.extensions)
                await response.aread()
                # Preserve rotated tokens before cancellation releases the auth lock.
                store_task = asyncio.create_task(asyncio.to_thread(self._store_refresh, response))
                cancelled = False
                while not store_task.done():
                    try:
                        await asyncio.shield(store_task)
                    except asyncio.CancelledError:
                        cancelled = True

                store_task.result()
                if cancelled:
                    raise asyncio.CancelledError

        yield self._authorize(request)


def device_login(tracker_url: str, http: httpx.Client, announce: Callable[[str, str], None]) -> LoginSession:
    tracker_url = tracker_url.rstrip("/")
    response = http.get(f"{tracker_url}/auth/cli")
    if response.status_code == 404:
        raise ValkyrieConfigError("This Tracker does not support `valkyrie login`. Set VALKYRIE_API_KEY instead.")
    settings = _read(_Settings, response, "Reading the Tracker's login settings")
    device = _read(
        _Device,
        http.post(settings.device_url, data={"client_id": settings.client_id, "scope": "openid"}),
        "Starting the device login",
    )
    announce(device.verification_uri_complete or device.verification_uri, device.user_code)
    interval, deadline = max(1.0, device.interval), time.time() + device.expires_in
    while time.time() < deadline:
        time.sleep(interval)
        response = http.post(
            settings.token_url,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": device.device_code,
                "client_id": settings.client_id,
            },
        )
        if response.is_success:
            tokens = _read(_Tokens, response, "Finishing the device login")
            return LoginSession(
                tracker_url=tracker_url,
                project_id=settings.project_id,
                refresh_url=settings.refresh_url,
                access_token=tokens.access_token,
                refresh_token=tokens.refresh_token,
                expires_at=_expiry(tokens.access_token, tokens.expires_in),
            )
        try:
            error = response.json().get("error")
        except (ValueError, AttributeError):
            error = None
        if error == "slow_down":
            interval += 5
        elif error == "access_denied":
            raise ValkyrieConfigError("The login was denied in the browser.")
        elif error == "expired_token":
            break
        elif error != "authorization_pending":
            raise ValkyrieConfigError(f"The login service rejected the device login ({response.status_code})")
    raise ValkyrieConfigError("The login code expired before it was approved. Run `valkyrie login` again.")
