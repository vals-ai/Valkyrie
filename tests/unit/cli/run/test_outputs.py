"""Tests for run output downloads.

Run: uv run pytest tests/unit/cli/run/test_outputs.py
"""

import io
import tarfile
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from click.testing import CliRunner

from valkyrie.cli.run.outputs import extract_run_outputs, outputs

from tests.unit.cli.conftest import MockTracker
from tests.unit.cli.factories import make_fetch_metadata, ok

_RUN_ID = UUID("123e4567-e89b-12d3-a456-426614174000")


def _tar(files: dict[str, bytes], mode: str = "w") -> bytes:
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode=mode) as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))

    return archive.getvalue()


def test_outputs_extract_into_a_named_directory_and_unpack_task_archives(
    tracker: MockTracker, cli_runner: CliRunner, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The default directory names the benchmark, agent and run; selected task IDs reach Tracker."""
    nested = _tar({"nested.txt": b"nested contents"}, mode="w:gz")
    tracker.reply("GET", f"/fetch-benchmark-metadata/{_RUN_ID}", ok(make_fetch_metadata(_RUN_ID)))
    tracker.reply(
        "GET",
        f"/fetch-run-outputs/{_RUN_ID}",
        httpx.Response(200, content=_tar({"task-a/output.txt": b"run output", "task-a/artifacts.tar.gz": nested})),
    )
    monkeypatch.chdir(tmp_path)

    result = cli_runner.invoke(outputs, [str(_RUN_ID), "--task-ids", "task-a, task-b"])

    assert result.exit_code == 0, result.output
    assert tracker.requested("GET", f"/fetch-run-outputs/{_RUN_ID}")[0].url.params.get_list("task_ids") == [
        "task-a",
        "task-b",
    ]

    output_dir = tmp_path / f"swebench_mini_sweagent_{_RUN_ID}"
    assert f"Run outputs extracted to: {output_dir.name}" in result.output
    assert (output_dir / "task-a" / "output.txt").read_bytes() == b"run output"
    assert (output_dir / "task-a" / "artifacts" / "nested.txt").read_bytes() == b"nested contents"
    assert not (output_dir / "task-a" / "artifacts.tar.gz").exists()


@pytest.mark.parametrize("nested", [False, True], ids=["outer-archive", "nested-archive"])
def test_extract_run_outputs_rejects_archive_traversal(nested: bool, tmp_path: Path) -> None:
    if nested:
        archive = _tar({"task/artifacts.tar.gz": _tar({"../../escaped.txt": b"escaped"}, mode="w:gz")})
        escaped_path = tmp_path / "output" / "escaped.txt"
    else:
        archive = _tar({"../escaped.txt": b"escaped"})
        escaped_path = tmp_path / "escaped.txt"

    with pytest.raises(tarfile.FilterError):
        extract_run_outputs(io.BytesIO(archive), tmp_path / "output")

    assert not escaped_path.exists()
