import tarfile
import tempfile
from pathlib import Path
from typing import BinaryIO
from uuid import UUID

import click
from valkyrie.sdk import ValkyrieClient

from valkyrie.cli.client import call
from valkyrie.cli.display import format_table, terminal_safe
from valkyrie.cli.run.task_ids import resolve_task_ids


@click.command(
    name="outputs",
    help="Fetch run outputs by run id. \n\nExample:\nvalkyrie run outputs 123e4567-e89b-12d3-a456-426614174000 --output-dir ./run_outputs",
)
@click.argument("run_id", type=UUID)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Directory to save run outputs (defaults to ./<benchmark>_<agent>_<run-id>)",
)
@click.option(
    "--task-ids",
    type=str,
    required=False,
    default=None,
    help="Comma-separated list of task IDs to download (e.g., astropy__astropy-7606,django__django-10880)",
)
def outputs(run_id: UUID, output_dir: Path | None, task_ids: str | None):
    """
    Fetch run outputs for a benchmark by its run id.

    Example:
        valkyrie run outputs 123e4567-e89b-12d3-a456-426614174000
        valkyrie run outputs 123e4567-e89b-12d3-a456-426614174000 --task-ids astropy__astropy-7606,django__django-10880
    """
    selected_task_ids = resolve_task_ids(task_ids)

    async def download(client: ValkyrieClient) -> Path:
        metadata = await client.runs.metadata(run_id)
        contract_name = metadata.benchmark_arguments.contract.name
        destination = output_dir or Path(f"{metadata.benchmark_name}_{contract_name}_{metadata.benchmark_id}")
        click.echo(f"\r\033[KFetching run outputs for run {run_id}...", nl=False)
        with tempfile.TemporaryFile() as archive:
            click.echo("\r\033[KDownloading...", nl=False)
            async for chunk in client.runs.stream_outputs(run_id, task_ids=selected_task_ids):
                archive.write(chunk)
            archive.seek(0)
            extract_run_outputs(archive, destination)
        return destination

    destination = call(download)
    click.echo(click.style(f"\r\033[K✓ Run outputs extracted to: {destination}", fg="green"))


@click.command(name="artifacts")
@click.argument("run_id", type=UUID)
@click.option("--prefix", default="", help="Relative file or directory path.")
@click.option("--cursor", default=None, help="Continue from a previous next_cursor.")
@click.option("--limit", type=click.IntRange(1, 1000), default=100, show_default=True)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"], case_sensitive=False),
    default="text",
    show_default=True,
)
def artifacts(run_id: UUID, prefix: str, cursor: str | None, limit: int, output_format: str) -> None:
    """List one page of artifact paths available within a run."""
    response = call(lambda client: client.artifacts.list(run_id, prefix=prefix, cursor=cursor, limit=limit))
    if output_format == "json":
        click.echo(response.model_dump_json(indent=2))
        return
    format_table(
        [
            {"Path": terminal_safe(entry.path, preserve_newlines=False), "Bytes": str(entry.size)}
            for entry in response.artifacts
        ],
        ["Path", "Bytes"],
        item_name="artifact",
    )
    if response.next_cursor is not None:
        click.echo(f"Next cursor: {terminal_safe(response.next_cursor, preserve_newlines=False)}")


@click.command(name="output", help="Download files from a benchmark run by its ID.")
@click.argument("benchmark_id", type=UUID)
@click.argument("subpath", type=str, default="", required=False)
@click.option(
    "-o",
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Directory to save downloaded files (defaults to ./<benchmark_id>)",
)
def output_path(benchmark_id: UUID, subpath: str, output_dir: Path | None):
    """
    Download a run artifact path through Tracker into a new local directory.

    Example:
        valkyrie run output 6f176c17-7199-4ebc-b931-973e5600c1c9
        valkyrie run output 6f176c17-7199-4ebc-b931-973e5600c1c9 astropy__astropy-7606
        valkyrie run output 6f176c17-7199-4ebc-b931-973e5600c1c9 swebench.json -o ./downloaded-artifacts
    """
    destination = output_dir if output_dir is not None else Path(str(benchmark_id))
    result = call(lambda client: client.artifacts.download(benchmark_id, destination, path=subpath))
    click.echo(click.style(f"✓ Run artifacts downloaded to: {result}", fg="green"))


def extract_run_outputs(archive: BinaryIO, output_dir: Path) -> None:
    """Extract a run's output tarball into `output_dir`, which may already exist.

    Each nested `*.tar.gz` (a task's agent output) is unpacked beside itself into a directory of
    the same name and then removed. tarfile's `data` filter rejects members that would escape.
    """
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    click.echo(f"\r\033[KExtracting archives to {output_dir}...", nl=False)
    with tarfile.open(fileobj=archive, mode="r") as tar:
        tar.extractall(output_dir, filter="data")

    nested_tars = list(output_dir.rglob("*.tar.gz"))
    if nested_tars:
        click.echo(f"\r\033[KUnpacking {len(nested_tars)} nested tar.gz files...", nl=False)
    for nested_tar in nested_tars:
        extract_dir = nested_tar.parent / nested_tar.stem.replace(".tar", "")
        extract_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(nested_tar, "r:gz") as tar:
            tar.extractall(extract_dir, filter="data")
        nested_tar.unlink()
