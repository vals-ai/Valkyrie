from pathlib import Path

import click

from valkyrie.cli.client import call


@click.command(
    name="tasks",
    help="Save task IDs for a benchmark dataset. \n\nExample:\nvalkyrie benchmark tasks swebench --dataset default",
)
@click.argument("benchmark_name", type=str)
@click.option(
    "--dataset",
    type=str,
    required=False,
    default=None,
    help="Dataset name to fetch from the benchmark service (defaults to 'default')",
)
@click.option(
    "--header",
    "-H",
    "headers",
    multiple=True,
    nargs=2,
    type=(str, str),
    help="Custom header for benchmark service requests (e.g., -H Authorization my-credential)",
)
@click.option(
    "--ignore-custom-services",
    "--ics",
    is_flag=True,
    help="Use the hosted benchmark service instead of the URL saved with `valkyrie benchmark service set`.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path, file_okay=True, dir_okay=False),
    required=False,
    default=None,
    help="Path to save task IDs, one per line. Defaults to <benchmark>-<dataset>-tasks.txt.",
)
def tasks(
    benchmark_name: str,
    dataset: str | None,
    headers: tuple[tuple[str, str]],
    ignore_custom_services: bool,
    output: Path | None,
):
    """
    Save task IDs for a benchmark dataset.
    """
    task_ids = call(
        lambda client: client.services.task_ids(
            benchmark_name,
            dataset=dataset,
            service_headers=dict(headers),
            ignore_custom_services=ignore_custom_services,
        )
    )
    output_path = output or Path(f"{benchmark_name}-{dataset or 'default'}-tasks.txt")
    output_path.write_text("\n".join(task_ids) + "\n", encoding="utf-8")
    click.echo(f"Wrote {len(task_ids)} task IDs to {output_path}")
