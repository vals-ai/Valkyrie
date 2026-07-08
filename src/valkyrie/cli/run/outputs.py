import asyncio
import tarfile
import tempfile
from pathlib import Path
from uuid import UUID

import click
from httpx import Response
from tracker.aws.s3 import S3_BENCHMARKS_PREFIX

from valkyrie.cli.exceptions import TrackerServiceError
from valkyrie.cli.run.artifacts import download_s3_path
from valkyrie.cli.run.task_ids import resolve_task_ids
from valkyrie.cli.tracker_client import TrackerService


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
    try:
        with TrackerService() as tracker:
            metadata = tracker.fetch_benchmark_metadata(run_id)

            if output_dir is None:
                output_dir = Path(
                    f"{metadata.benchmark_name}_{metadata.benchmark_arguments.contract.name}_{metadata.benchmark_id}"
                )

            click.echo(f"\r\033[KFetching run outputs for run {run_id}...", nl=False)

            response = tracker.fetch_run_outputs(
                run_id,
                task_ids=resolve_task_ids(task_ids),
            )

            download_run_outputs(response, output_dir)

            click.echo(click.style(f"\r\033[K✓ Run outputs extracted to: {output_dir}", fg="green"))

    except TrackerServiceError as e:
        raise click.ClickException(str(e))


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
    Download all files under a benchmark's S3 directory.

    Example:
        valkyrie run output 6f176c17-7199-4ebc-b931-973e5600c1c9
        valkyrie run output 6f176c17-7199-4ebc-b931-973e5600c1c9 astropy__astropy-7606
        valkyrie run output 6f176c17-7199-4ebc-b931-973e5600c1c9 swebench.json -o .
    """
    try:
        path = f"{S3_BENCHMARKS_PREFIX}/{benchmark_id}"
        if subpath:
            path = f"{path}/{subpath.strip('/')}"

        if output_dir is None:
            output_dir = Path(str(benchmark_id))

        click.echo(f"\r\033[KDownloading from s3://{path}...", nl=False)
        count = asyncio.run(download_s3_path(path, output_dir))
        click.echo(click.style(f"\r\033[K✓ {count} file(s) downloaded to: {output_dir}", fg="green"))
    except Exception as e:
        click.echo(click.style(f"✗ Error: {e}", fg="red"), err=True)
        raise click.Abort()


def download_run_outputs(run_outputs_response: Response, output_dir: Path) -> None:
    """Download run outputs from a response and extract them to a directory."""
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(suffix=".tar", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
        click.echo("\r\033[KDownloading...", nl=False)

        for chunk in run_outputs_response.iter_bytes():
            tmp_file.write(chunk)

    click.echo(f"\r\033[KExtracting archives to {output_dir}...", nl=False)

    with tarfile.open(tmp_path, "r") as tar:
        tar.extractall(output_dir)

    nested_tars = list(output_dir.rglob("*.tar.gz"))
    if nested_tars:
        click.echo(f"\r\033[KUnpacking {len(nested_tars)} nested tar.gz files...", nl=False)

        for nested_tar in nested_tars:
            extract_dir = nested_tar.parent / nested_tar.stem.replace(".tar", "")
            extract_dir.mkdir(parents=True, exist_ok=True)

            with tarfile.open(nested_tar, "r:gz") as tar:
                tar.extractall(extract_dir)

            nested_tar.unlink()

    tmp_path.unlink()
