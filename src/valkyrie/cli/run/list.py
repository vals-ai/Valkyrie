import click
from tracker.database.models import BenchmarkStatus
from tracker.types import Order

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.tracker_service import TrackerService
from valkyrie.cli.utils import check_tracker_service_health, paginate_benchmarks


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
def list_benchmarks(
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
            if not check_tracker_service_health(tracker):
                return

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
