import click

from valkyrie.cli.lazy import LazyGroup


@click.group(
    cls=LazyGroup,
    lazy_commands={
        "analyze": (
            "valkyrie.cli.run.analyze:analyze",
            "Trigger Docent ingestion + error analysis for a finished run.",
        ),
        "errors": ("valkyrie.cli.run.errors:errors", "Show stored run and current task error messages."),
        "fetch": ("valkyrie.cli.run.fetch:fetch", "Fetch a run by its run id."),
        "filter-options": (
            "valkyrie.cli.run.filter_options:filter_options",
            "List benchmark, agent, model, dataset, and starter values from your run history.",
        ),
        "list": ("valkyrie.cli.run.list_runs:list_runs", "List runs by providing filter values."),
        "logs": ("valkyrie.cli.run.logs:logs", "Fetch or follow logs for a run or one task."),
        "artifacts": ("valkyrie.cli.run.outputs:artifacts", "List one page of artifact paths available within a run."),
        "output": ("valkyrie.cli.run.outputs:output_path", "Download files from a benchmark run by its ID."),
        "outputs": ("valkyrie.cli.run.outputs:outputs", "Fetch run outputs by run id."),
        "results": ("valkyrie.cli.run.results:results", "Retrieve run results by its run id."),
        "resume": ("valkyrie.cli.run.resume:resume", "Resume a run by its run id."),
        "retry": ("valkyrie.cli.run.resume:retry_command", "Retry a run's errored tasks by its run id."),
        "shed": (
            "valkyrie.cli.run.shed:shed",
            "Lower concurrency for an active run, then force stop the newest building or in-progress tasks until the "
            "run is back at the new limit.",
        ),
        "start": ("valkyrie.cli.run.start:start", "Start a run."),
        "status": ("valkyrie.cli.run.status:status_runs", "Fetch lightweight status for multiple run IDs."),
        "stop": ("valkyrie.cli.run.stop:stop", "Stop a run by its run id."),
        "update": ("valkyrie.cli.run.update:update", "Update concurrency for an active run."),
        "tasks": ("valkyrie.cli.run.tasks:tasks", "List one page of tasks, including their status and error messages."),
        "task": ("valkyrie.cli.run.tasks:task", "Inspect one task's status, evaluation result, and failure reason."),
        "task-artifacts": (
            "valkyrie.cli.run.tasks:task_artifacts",
            "Get temporary download and log links for one task.",
        ),
    },
)
def run():
    """Start, monitor, manage, and retrieve runs."""


__all__ = ["run"]
