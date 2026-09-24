"""Run: uv run --project services/tracker pytest tmp/test_compare.py.

Check real Git snapshot isolation and the comparison's failure classifications.
"""

import subprocess
import os
import sys
from pathlib import Path

import pytest

from tmp.compare import compare_exit, run_command, snapshot


class TestComparison:
    """A setup failure or an unreproduced baseline must never count as a fix."""

    @pytest.mark.parametrize(
        ("before", "after", "expected"),
        [
            ("interrupted", "survived", 0),
            ("setup_error", "survived", 2),
            ("interrupted", "probe_error", 2),
            ("interrupted", "cleanup_error", 2),
            ("interrupted", "interrupted", 1),
            ("survived", "survived", 3),
        ],
    )
    def test_outcomes(self, before: str, after: str, expected: int) -> None:
        """Classify observed results without treating infrastructure errors as interruptions.

        Test cases:
        - Only a reproduced interruption followed by survival is a successful comparison.
        - Setup, probe, and cleanup failures produce an inconclusive result.
        """
        assert (
            compare_exit(
                [
                    {"label": "before", "scenario": "replacement", "outcome": before},
                    {"label": "after", "scenario": "replacement", "outcome": after},
                ]
            )
            == expected
        )

    def test_missing_results_and_negative_control(self) -> None:
        """Reject incomplete comparisons and keep the explicit unsafe-change control separate.

        Test cases:
        - Missing revisions or the replacement scenario cannot report success.
        - The unsafe-change control must interrupt both revisions.
        """
        assert compare_exit([]) == 2
        assert compare_exit([{"label": "before", "outcome": "interrupted"}]) == 2
        assert (
            compare_exit(
                [
                    {"label": "before", "scenario": "maintenance", "outcome": "interrupted"},
                    {"label": "after", "scenario": "maintenance", "outcome": "interrupted"},
                ]
            )
            == 2
        )
        assert (
            compare_exit(
                [
                    {"label": "before", "scenario": "replacement", "outcome": "interrupted"},
                    {"label": "after", "scenario": "replacement", "outcome": "survived"},
                    {"label": "before", "scenario": "maintenance", "outcome": "interrupted"},
                    {"label": "after", "scenario": "maintenance", "outcome": "interrupted"},
                ]
            )
            == 0
        )

    def test_snapshots_do_not_change_checkout_or_copy_untracked_secrets(self, tmp_path: Path) -> None:
        """Snapshot a commit and tracked local edits without changing the original checkout.

        Test cases:
        - Commit snapshots retain committed source; working-tree snapshots include modifications and deletions.
        - Untracked credentials and the harness's output directory are excluded.
        """
        repo = tmp_path / "repo"
        repo.mkdir()
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        (repo / "source.py").write_text("old\n")
        (repo / "deleted.py").write_text("old\n")
        subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(repo),
                "-c",
                "user.name=Comparison Test",
                "-c",
                "user.email=comparison@example.invalid",
                "commit",
                "-qm",
                "baseline",
            ],
            check=True,
        )
        (repo / "source.py").write_text("new\n")
        (repo / "deleted.py").unlink()
        (repo / ".env").write_text("NOT_A_REAL_SECRET=test\n")
        before = snapshot(repo, "HEAD", tmp_path / "before")
        after = snapshot(repo, "working-tree", tmp_path / "after")

        assert (tmp_path / "before/source.py").read_text() == "old\n"
        assert (tmp_path / "after/source.py").read_text() == "new\n"
        assert not (tmp_path / "after/deleted.py").exists()
        assert not (tmp_path / "after/.env").exists()
        assert (repo / "source.py").read_text() == "new\n"
        assert before["commit"] == after["commit"]
        assert before["source_digest"] != after["source_digest"]

    def test_timeout_allows_child_cleanup(self, tmp_path: Path) -> None:
        """Let an interrupted probe finish cleanup before reporting a timeout.

        Test cases:
        - A real child receives interruption and writes its cleanup acknowledgement.
        - The driver still reports timeout rather than success.
        """
        marker = tmp_path / "cleaned"
        child = "import time, sys\nfrom pathlib import Path\ntry:\n    time.sleep(30)\nfinally:\n    Path(sys.argv[1]).write_text('cleaned')\n"
        with pytest.raises(subprocess.TimeoutExpired):
            run_command(
                [sys.executable, "-c", child, str(marker)],
                tmp_path,
                tmp_path / "child.log",
                dict(os.environ),
                timeout=1,
            )
        assert marker.read_text() == "cleaned"
