import click
from tracker.types import ListRunsRequest, ListRunsResponse, Order, RunStatus, RunSummary

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.display import format_table, paginate_cli_pages, short_local_time
from valkyrie.cli.run.progress import RunFormatter
from valkyrie.cli.run.snapshot import format_run_list_json
from valkyrie.cli.tracker_client import TrackerService

_MACHINE_PAGE_LIMIT = 500


@click.command(
    name="list",
    help="List runs by providing filter values. \n\nExample:\nvalkyrie run list --agent-name claude_code --benchmark-name swebench --status IN_PROGRESS --order-by DESC",
)
@click.option(
    "--agent-name",
    type=str,
    required=False,
    help="Agent name (e.g., claude_code)",
)
@click.option(
    "--benchmark-name",
    type=str,
    required=False,
    help="Benchmark name (e.g., swebench)",
)
@click.option(
    "--model",
    type=str,
    required=False,
    help="Model name (e.g., anthropic/claude-sonnet-4-20250514)",
)
@click.option(
    "--dataset",
    type=str,
    required=False,
    help="Dataset name (e.g., default, terminal-bench-2.1)",
)
@click.option(
    "--label",
    "-l",
    type=str,
    required=False,
    help="Run label",
)
@click.option(
    "--status",
    type=click.Choice([option.value for option in RunStatus], case_sensitive=False),
    required=False,
    default=None,
    help="Status of the runs to fetch (e.g., in_progress, finished, error)",
)
@click.option(
    "--order-by",
    type=click.Choice([option.value for option in Order], case_sensitive=False),
    required=False,
    default=Order.DESC.value,
    help="Order of the runs to fetch (e.g., desc, asc)",
)
@click.option(
    "--started-by",
    type=str,
    required=False,
    default=None,
    help="Comma-separated list of starter emails (e.g., alice@vals.ai,bob@vals.ai). Case-insensitive.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
    help="Output format. Machine-readable JSON requires --all.",
)
@click.option(
    "--all",
    "all_runs",
    is_flag=True,
    help="Fetch every matching run without interactive paging. Requires --format json.",
)
def list_runs(
    agent_name: str | None,
    benchmark_name: str | None,
    model: str | None,
    dataset: str | None,
    label: str | None,
    status: str | None,
    order_by: str = "desc",
    started_by: str | None = None,
    output_format: str = "text",
    all_runs: bool = False,
):
    """
    List runs based on the request parameters.

    Use vim keys to navigate: [h] previous page, [l] next page, [q] quit.

    Example:
        valkyrie run list --agent-name claude_code --benchmark-name swebench --status IN_PROGRESS --order-by DESC
    """
    started_by_list: list[str] = [s.strip() for s in started_by.split(",") if s.strip()] if started_by else []

    if output_format == "json" and not all_runs:
        raise click.UsageError("--format json requires --all.")
    if all_runs and output_format != "json":
        raise click.UsageError("--all requires --format json.")

    try:
        with TrackerService() as tracker:
            if output_format == "json":
                request = _build_list_runs_request(
                    agent_name,
                    benchmark_name,
                    model,
                    dataset,
                    label,
                    status,
                    order_by,
                    started_by_list or None,
                    cursor="",
                    limit=_MACHINE_PAGE_LIMIT,
                )
                click.echo(format_run_list_json(fetch_all_runs(tracker, request)))
            else:
                paginate_runs(
                    tracker,
                    agent_name,
                    benchmark_name,
                    model,
                    dataset,
                    label,
                    status,
                    order_by,
                    started_by=started_by_list or None,
                )
    except TrackerServiceError as e:
        raise click.ClickException(str(e))


def _build_list_runs_request(
    agent_name: str | None,
    benchmark_name: str | None,
    model: str | None,
    dataset: str | None,
    label: str | None,
    status: str | None,
    order_by: str,
    started_by: list[str] | None,
    *,
    cursor: str | None = None,
    limit: int = 5,
    offset: int = 0,
) -> ListRunsRequest:
    """Build a list request shared by human and machine output modes."""
    return ListRunsRequest(
        agent_name=[agent_name] if agent_name else None,
        benchmark_name=[benchmark_name] if benchmark_name else None,
        model=model,
        dataset=dataset,
        label=label,
        status=[RunStatus(status)] if status else None,
        started_by=started_by,
        order_by=Order(order_by),
        cursor=cursor,
        limit=limit,
        offset=offset,
    )


def fetch_all_runs(tracker: TrackerService, request: ListRunsRequest) -> list[RunSummary]:
    """Exhaust cursor pagination before writing any machine-readable output."""
    runs: list[RunSummary] = []
    cursor = request.cursor
    seen_cursors: set[str] = set()

    while cursor is not None:
        if cursor in seen_cursors:
            raise TrackerServiceError("Tracker returned a repeated run-list cursor.")
        seen_cursors.add(cursor)

        response = tracker.list_runs(request.model_copy(update={"cursor": cursor}))
        runs.extend(response.runs)
        cursor = response.next_cursor

    return runs


def format_list_runs_response(
    list_runs_response: ListRunsResponse,
    current_page: int = 1,
    total_pages: int = 1,
) -> None:
    """Format and display runs in a table format."""
    runs = list_runs_response.runs

    if not runs:
        click.echo(click.style("No runs found.", fg="yellow"))
        return

    def format_score(score: float | None) -> str:
        return f"{score:.1f}%" if score is not None else "-"

    rows: list[dict[str, str]] = []
    for run in runs:
        _, progress_percentage = RunFormatter.create_progress_bar(run.finished_tasks, run.total_tasks)

        rows.append(
            {
                "ID": str(run.run_id),
                "Benchmark": run.benchmark_name,
                "Agent": run.agent_name,
                "Label": run.label or "-",
                "Started By": run.started_by_email or "—",
                "Model": run.model or "-",
                "Dataset": run.dataset or "default",
                "Status": click.style(
                    run.status.value.replace("_", " ").title(),
                    fg=RunFormatter.STATUS_COLORS[run.status.value],
                ),
                "Score": format_score(run.final_score),
                "Started / Finished": f"{short_local_time(run.started_at)} / {short_local_time(run.finished_at, include_date=False) if run.finished_at else '-'}",
                "Progress": f"{progress_percentage:.1f}%",
            }
        )

    format_table(
        rows,
        [
            "ID",
            "Benchmark",
            "Agent",
            "Label",
            "Started By",
            "Model",
            "Dataset",
            "Status",
            "Score",
            "Started / Finished",
            "Progress",
        ],
        current_page,
        total_pages,
        list_runs_response.total_count,
        "run",
    )


def format_no_runs_found(
    agent_name: str | None,
    benchmark_name: str | None,
    model: str | None,
    dataset: str | None,
    label: str | None,
    status: str | None,
    started_by: list[str] | None = None,
) -> None:
    """Print the no-results message and active filters."""
    click.echo()
    click.echo(click.style("No runs found matching the specified filters.", fg="yellow"))
    click.echo()
    if any([agent_name, benchmark_name, model, dataset, label, status, started_by]):
        click.echo("Filters applied:")
        if agent_name:
            click.echo(f"  • Agent: {agent_name}")
        if benchmark_name:
            click.echo(f"  • Benchmark: {benchmark_name}")
        if model:
            click.echo(f"  • Model: {model}")
        if dataset:
            click.echo(f"  • Dataset: {dataset}")
        if label:
            click.echo(f"  • Label: {label}")
        if status:
            click.echo(f"  • Status: {status}")
        if started_by:
            click.echo(f"  • Started By: {', '.join(started_by)}")


def paginate_runs(
    tracker: TrackerService,
    agent_name: str | None,
    benchmark_name: str | None,
    model: str | None,
    dataset: str | None,
    label: str | None,
    status: str | None,
    order_by: str,
    limit: int = 5,
    started_by: list[str] | None = None,
) -> None:
    """Interactively page through runs."""

    def load_page(offset: int, page_limit: int) -> tuple[int, ListRunsResponse]:
        request = _build_list_runs_request(
            agent_name,
            benchmark_name,
            model,
            dataset,
            label,
            status,
            order_by,
            started_by,
            limit=page_limit,
            offset=offset,
        )
        response = tracker.list_runs(request)
        return response.total_count or 0, response

    def render_page(
        response: ListRunsResponse,
        current_page: int,
        total_pages: int,
        _total_count: int,
    ) -> None:
        format_list_runs_response(response, current_page, total_pages)

    paginate_cli_pages(
        load_page,
        render_page,
        limit=limit,
        render_empty=lambda: format_no_runs_found(
            agent_name, benchmark_name, model, dataset, label, status, started_by
        ),
    )
