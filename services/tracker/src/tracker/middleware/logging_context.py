"""Taskiq middleware that sets logging context vars for worker jobs."""

from typing import Any, cast

from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

from tracker.logging import benchmark_id_var, request_id_var, task_id_var


class LoggingContextMiddleware(TaskiqMiddleware):
    """Sets logging context vars for Taskiq worker jobs."""

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        request_id_var.set("")
        benchmark_id_var.set("")
        task_id_var.set("")

        message_kwargs = message.kwargs
        benchmark_id_str = message_kwargs.get("benchmark_id_str", "")
        if not benchmark_id_str:
            execution_context = message_kwargs.get("execution_context_json")
            if isinstance(execution_context, dict):
                benchmark_id_str = cast(dict[str, Any], execution_context).get("benchmark_id", "")
        if benchmark_id_str:
            benchmark_id_var.set(str(benchmark_id_str))

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
