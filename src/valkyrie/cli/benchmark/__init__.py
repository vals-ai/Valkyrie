import click

from valkyrie.cli.lazy import LazyGroup


@click.group(
    cls=LazyGroup,
    lazy_commands={
        "tasks": ("valkyrie.cli.benchmark.tasks:tasks", "Save task IDs for a benchmark dataset."),
        "service": (
            "valkyrie.cli.benchmark.services:service",
            "Save benchmark service URLs that your runs use instead of the hosted services.",
        ),
    },
)
def benchmark():
    """Export the task IDs in a benchmark dataset, and save benchmark services."""


__all__ = ["benchmark"]
