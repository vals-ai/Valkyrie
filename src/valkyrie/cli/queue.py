"""Inspect the organization's sandbox admission queue."""

import asyncio

import click
from valkyrie.sdk import SchedulerOverviewResponse, ValkyrieClient, ValkyrieSDKError

from valkyrie.cli.display import format_table, terminal_safe
from valkyrie.cli.runtime_config import config_location, tracker_service_url


@click.group()
def queue() -> None:
    """Inspect your organization's sandbox scheduler."""


@queue.command()
@click.option(
    "--waiting-limit", type=click.IntRange(1, 200), default=100, show_default=True, help="Maximum waiting task entries."
)
@click.option(
    "--active-limit", type=click.IntRange(1, 200), default=100, show_default=True, help="Maximum active task entries."
)
@click.option(
    "--waiting-offset", type=click.IntRange(min=0), default=0, show_default=True, help="Waiting entries to skip."
)
@click.option(
    "--active-offset", type=click.IntRange(min=0), default=0, show_default=True, help="Active entries to skip."
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format. JSON includes task pages, capped flags, and next offsets.",
)
def status(waiting_limit: int, active_limit: int, waiting_offset: int, active_offset: int, output_format: str) -> None:
    """Show live queue priorities, positions, and active tasks for your organization."""
    try:
        response = asyncio.run(_fetch_overview(waiting_limit, active_limit, waiting_offset, active_offset))
    except (ValkyrieSDKError, ValueError) as error:
        raise click.ClickException(str(error)) from error

    if output_format == "json":
        click.echo(response.model_dump_json(indent=2))
        return

    _format_overview(response)


async def _fetch_overview(
    waiting_limit: int, active_limit: int, waiting_offset: int, active_offset: int
) -> SchedulerOverviewResponse:
    async with ValkyrieClient.from_config(config_location(), base_url=tracker_service_url()) as client:
        return await client.scheduler.overview(
            waiting_limit=waiting_limit,
            active_limit=active_limit,
            waiting_offset=waiting_offset,
            active_offset=active_offset,
        )


def _format_overview(response: SchedulerOverviewResponse) -> None:
    summary = response.summary
    click.echo(f"Observed at: {response.observed_at.isoformat()}")
    click.echo(
        f"Waiting: {summary.waiting} | Building: {summary.building} | "
        f"In progress: {summary.in_progress} | Evaluating: {summary.evaluating}"
    )

    if response.pools:
        format_table(
            [
                {"Pool": terminal_safe(pool.pool_id, preserve_newlines=False), "Waiting": str(pool.waiting)}
                for pool in response.pools
            ],
            ["Pool", "Waiting"],
            item_name="pool",
        )

    click.echo("\nWaiting tasks (P0 highest; position is within each pool)")
    format_table(
        [
            {
                "Pool": terminal_safe(entry.pool_id, preserve_newlines=False),
                "Position": str(entry.position),
                "Priority": f"P{entry.priority}",
                "Run": str(entry.benchmark_uuid),
                "Task": terminal_safe(entry.external_task_id, preserve_newlines=False),
                "Wait (s)": f"{max(0, (response.observed_at - entry.enqueued_at).total_seconds()):.0f}",
            }
            for entry in response.waiting_entries
        ],
        ["Pool", "Position", "Priority", "Run", "Task", "Wait (s)"],
        total_count=summary.waiting,
        item_name="waiting task",
    )
    if response.waiting_capped:
        click.echo(f"Showing {len(response.waiting_entries)} of {summary.waiting} waiting tasks; entries are capped.")
    if response.waiting_next_offset is not None:
        click.echo(f"Next waiting page: --waiting-offset {response.waiting_next_offset}")

    click.echo("\nActive tasks")
    format_table(
        [
            {
                "Run": str(entry.benchmark_uuid),
                "Task": terminal_safe(entry.external_task_id, preserve_newlines=False),
                "Status": entry.status.value,
            }
            for entry in response.active_entries
        ],
        ["Run", "Task", "Status"],
        total_count=summary.building + summary.in_progress + summary.evaluating,
        item_name="active task",
    )
    if response.active_capped:
        click.echo(f"Showing {len(response.active_entries)} active tasks; entries are capped.")
    if response.active_next_offset is not None:
        click.echo(f"Next active page: --active-offset {response.active_next_offset}")
    if response.waiting_next_offset is not None or response.active_next_offset is not None:
        click.echo("Pages are live; queue changes between calls can repeat or skip entries.")
