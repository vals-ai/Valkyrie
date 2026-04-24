"""Taskiq middleware that extracts OTel trace context from message labels."""

from contextvars import Token
from typing import Any

from opentelemetry.context import Context, attach, detach
from opentelemetry.propagate import extract
from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

# Union of keys written by the composite propagator (W3C + Sentry).
# Hardcoded rather than derived at import — the global propagator is set later at startup.
_TRACE_CONTEXT_KEYS = frozenset({"traceparent", "tracestate", "sentry-trace", "baggage"})


class TracingContextMiddleware(TaskiqMiddleware):
    """Extracts OTel trace context from Taskiq message labels.

    Must be registered before LoggingContextMiddleware so the trace context is
    active when logging context vars are set.
    """

    def __init__(self) -> None:
        super().__init__()
        # Keyed by task_id so we survive a downstream middleware swapping the message object.
        self._active_tokens: dict[str, Token[Context]] = {}

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        carrier = {k: v for k, v in message.labels.items() if k in _TRACE_CONTEXT_KEYS}
        if carrier:
            ctx = extract(carrier)
            token = attach(ctx)
            self._active_tokens[message.task_id] = token
        return message

    async def post_execute(self, message: TaskiqMessage, result: TaskiqResult[Any]) -> None:
        self._detach(message.task_id)

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
        exception: BaseException,
    ) -> None:
        self._detach(message.task_id)

    def _detach(self, task_id: str) -> None:
        token = self._active_tokens.pop(task_id, None)
        if token is not None:
            detach(token)
