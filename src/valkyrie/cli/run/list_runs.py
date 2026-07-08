import click
from tracker.database.models import BenchmarkStatus
from tracker.types import FetchBenchmarksRequest, FetchBenchmarksResponse, Order

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.display import format_table, short_local_time
from valkyrie.cli.run.progress import BenchmarkFormatter
from valkyrie.cli.tracker_client import TrackerService


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
    type=click.Choice([option.value for option in BenchmarkStatus], case_sensitive=False),
    required=False,
    default=None,
    help="Status of the benchmarks to fetch (e.g., in_progress, finished, error)",
)
@click.option(
    "--order-by",
    type=click.Choice([option.value for option in Order], case_sensitive=False),
    required=False,
    default=Order.DESC.value,
    help="Order by the benchmarks to fetch (e.g., desc, asc)",
)
@click.option(
    "--started-by",
    type=str,
    required=False,
    default=None,
    help="Comma-separated list of starter emails (e.g., alice@vals.ai,bob@vals.ai). Case-insensitive.",
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
):
    """
    List runs based on the request parameters.

    Use vim keys to navigate: [h] previous page, [l] next page, [q] quit.

    Example:
        valkyrie run list --agent-name claude_code --benchmark-name swebench --status IN_PROGRESS --order-by DESC
    """
    started_by_list: list[str] = [s.strip() for s in started_by.split(",") if s.strip()] if started_by else []

    try:
        with TrackerService() as tracker:
            paginate_benchmarks(
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


def format_fetch_benchmarks_response(
    fetch_benchmarks_response: FetchBenchmarksResponse,
    current_page: int = 1,
    total_pages: int = 1,
) -> None:
    """Format and display runs in a table format."""
    benchmarks = fetch_benchmarks_response.benchmarks

    if not benchmarks:
        click.echo(click.style("No runs found.", fg="yellow"))
        return

    def format_score(score: float | None) -> str:
        return f"{score:.1f}%" if score is not None else "-"

    rows: list[dict[str, str]] = []
    for benchmark in benchmarks:
        _, progress_percentage = BenchmarkFormatter.create_progress_bar(benchmark.finished_tasks, benchmark.total_tasks)

        rows.append(
            {
                "ID": str(benchmark.id),
                "Benchmark": benchmark.name,
                "Agent": benchmark.agent_name,
                "Label": benchmark.label or "-",
                "Started By": benchmark.started_by_email or "—",
                "Model": benchmark.model or "-",
                "Dataset": benchmark.dataset or "default",
                "Status": click.style(
                    benchmark.status.value.replace("_", " ").title(),
                    fg=BenchmarkFormatter.STATUS_COLORS[benchmark.status.value],
                ),
                "Score": format_score(benchmark.final_score),
                "Started / Finished": f"{short_local_time(benchmark.started_at)} / {short_local_time(benchmark.finished_at, include_date=False) if benchmark.finished_at else '-'}",
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
        fetch_benchmarks_response.total_count,
        "run",
    )


def format_no_benchmarks_found(
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


def _clear_pager() -> None:
    click.echo("\033[2J\033[3J\033[1;1H", nl=False, color=True)


def paginate_benchmarks(
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
    current_page = 1
    offset = 0

    while True:
        request = FetchBenchmarksRequest(
            agent_name=[agent_name] if agent_name else None,
            benchmark_name=[benchmark_name] if benchmark_name else None,
            model=model,
            dataset=dataset,
            label=label,
            status=[BenchmarkStatus(status)] if status else None,
            started_by=started_by,
            order_by=Order(order_by),
            limit=limit,
            offset=offset,
        )

        response = tracker.fetch_benchmarks(request)
        total_count = response.total_count or 0
        total_pages = max(1, (total_count + limit - 1) // limit)

        _clear_pager()

        if total_count == 0:
            format_no_benchmarks_found(agent_name, benchmark_name, model, dataset, label, status, started_by)
            break

        format_fetch_benchmarks_response(response, current_page, total_pages)

        if total_pages <= 1:
            break

        char = click.getchar()

        if char == "l" and current_page < total_pages:
            current_page += 1
            offset += limit
        elif char == "h" and current_page > 1:
            current_page -= 1
            offset -= limit
        elif char == "q" or char == "\x03":
            break
