"""Taskiq middleware that sets logging context vars for executor jobs."""

from typing import Any

from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

from executor_protocol import executor_payload_benchmark_id
from tracker.logging import benchmark_id_var, request_id_var, task_id_var


class LoggingContextMiddleware(TaskiqMiddleware):
    """Set logging context variables during Taskiq task execution."""

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        request_id_var.set("")
        benchmark_id_var.set("")
        task_id_var.set("")

        benchmark_id = executor_payload_benchmark_id(message.kwargs)
        if benchmark_id:
            benchmark_id_var.set(benchmark_id)

        request_id = message.labels.get("request_id", "")
        if request_id:
            request_id_var.set(request_id)

        return message

    async def post_execute(self, message: TaskiqMessage, result: TaskiqResult[Any]) -> None:
        self._clear()

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
        exception: BaseException,
    ) -> None:
        self._clear()

    def _clear(self) -> None:
        request_id_var.set("")
        benchmark_id_var.set("")
        task_id_var.set("")
