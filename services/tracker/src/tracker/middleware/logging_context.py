"""Taskiq middleware that sets logging context vars for worker jobs."""

from typing import Any

from taskiq import TaskiqMessage, TaskiqMiddleware, TaskiqResult

from tracker.logging import request_id_var, run_id_var, task_id_var


class LoggingContextMiddleware(TaskiqMiddleware):
    """Set logging context variables during Taskiq task execution."""

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        request_id_var.set("")
        run_id_var.set("")
        task_id_var.set("")

        run_id = message.kwargs.get("benchmark_id_str", "")
        if run_id:
            run_id_var.set(run_id)

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
        run_id_var.set("")
        task_id_var.set("")
