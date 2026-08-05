"""Taskiq middleware that sets logging context vars for executor jobs."""

from typing import Any, cast

from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

from tracker.logging import benchmark_id_var, request_id_var, task_id_var


def _benchmark_id_from_message(message: TaskiqMessage) -> str:
    benchmark_id = message.kwargs.get("benchmark_id_str")
    if not benchmark_id:
        execution_context = message.kwargs.get("execution_context_json")
        if isinstance(execution_context, dict):
            benchmark_id = cast(dict[str, Any], execution_context).get("benchmark_id")
    return str(benchmark_id) if benchmark_id else ""


class LoggingContextMiddleware(TaskiqMiddleware):
    """Set logging context variables during Taskiq task execution."""

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        request_id_var.set("")
        benchmark_id_var.set("")
        task_id_var.set("")

        benchmark_id = _benchmark_id_from_message(message)
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
