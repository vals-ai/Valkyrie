"""ASGI middleware restricting a local Tracker to loopback Host headers."""

from typing import TYPE_CHECKING

from starlette.middleware.trustedhost import TrustedHostMiddleware

from tracker.local import config as local_config

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Receive, Scope, Send

LOCAL_ALLOWED_HOSTS = ["127.0.0.1", "localhost"]


class LocalTrustedHostMiddleware:
    """Reject non-loopback Host headers in local mode; a 127.0.0.1 bind alone does not stop DNS rebinding."""

    def __init__(self, app: "ASGIApp"):
        self.app = app
        self.trusted = TrustedHostMiddleware(app, allowed_hosts=LOCAL_ALLOWED_HOSTS)

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if local_config.resources is None:
            await self.app(scope, receive, send)
            return
        await self.trusted(scope, receive, send)
