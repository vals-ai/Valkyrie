"""Taskiq middleware that propagates OTel trace context from API to worker.

Trace context is injected into Taskiq message labels by the API endpoint
(via opentelemetry.propagate.inject) and extracted here on the worker side
so that worker spans become children of the original API trace.

Known limitation: Taskiq does not wrap the pre_execute chain in try/finally.
If a downstream middleware's pre_execute raises after this one has attached
a token, post_execute and on_error will not run and the token will be
orphaned in _active_tokens. None of the current middlewares raise in
pre_execute, so this is latent.
"""

from contextvars import Token
from typing import Any

from opentelemetry.context import Context, attach, detach
from opentelemetry.propagate import extract
from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

# Keys that OTel W3C TraceContext propagator writes into carriers
_TRACE_CONTEXT_KEYS = frozenset({"traceparent", "tracestate"})


class TracingContextMiddleware(TaskiqMiddleware):
    """Extracts OTel trace context from Taskiq message labels.

    Must be registered on the broker BEFORE LoggingContextMiddleware so
    that the trace context is active when logging context vars are set.
    """

    def __init__(self) -> None:
        super().__init__()
        # Keyed by message.task_id (a stable string unique per execution)
        # so we're robust against a downstream middleware replacing the
        # message object in pre_execute.
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
