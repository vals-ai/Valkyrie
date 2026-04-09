"""ASGI middleware for request correlation IDs."""

from typing import TYPE_CHECKING
from uuid import uuid4

from tracker.logging import benchmark_id_var, request_id_var, task_id_var

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send


class RequestContextMiddleware:
    """Pure ASGI middleware that generates a request_id per HTTP request."""

    def __init__(self, app: "ASGIApp"):
        self.app = app

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = uuid4().hex[:12]
        tokens = [
            request_id_var.set(request_id),
            benchmark_id_var.set(""),
            task_id_var.set(""),
        ]

        async def send_with_request_id(message: "Message") -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append((b"x-request-id", request_id.encode()))  # type: ignore[arg-type]
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            for token in tokens:
                token.var.reset(token)
