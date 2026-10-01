"""ASGI middleware that rejects oversized start, retry and resume request bodies."""

import re
from typing import TYPE_CHECKING

from fastapi import HTTPException
from starlette.responses import JSONResponse

if TYPE_CHECKING:
    from starlette.types import ASGIApp, Message, Receive, Scope, Send

MAX_RUN_REQUEST_BYTES = 1024 * 1024
_RUN_REQUEST_PATH = re.compile(r"/(start-benchmark|start-benchmark-with-storage|retry-or-resume-benchmark/[^/]+)")
_TOO_LARGE_DETAIL = f"Request body exceeds {MAX_RUN_REQUEST_BYTES} bytes"


class RunRequestSizeLimitMiddleware:
    """Return 413 before parsing when a run request body exceeds MAX_RUN_REQUEST_BYTES."""

    def __init__(self, app: "ASGIApp"):
        self.app = app

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope["type"] != "http" or scope["method"] != "POST" or not _RUN_REQUEST_PATH.fullmatch(scope["path"]):
            await self.app(scope, receive, send)
            return

        declared_length = dict(scope["headers"]).get(b"content-length")
        if declared_length is not None and int(declared_length) > MAX_RUN_REQUEST_BYTES:
            await JSONResponse({"detail": _TOO_LARGE_DETAIL}, status_code=413)(scope, receive, send)
            return

        received = 0

        async def limited_receive() -> "Message":
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > MAX_RUN_REQUEST_BYTES:
                    # FastAPI re-raises HTTPException from body reads, so this becomes a 413 response.
                    raise HTTPException(status_code=413, detail=_TOO_LARGE_DETAIL)
            return message

        await self.app(scope, limited_receive, send)
