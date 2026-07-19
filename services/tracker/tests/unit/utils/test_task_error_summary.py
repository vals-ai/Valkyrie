"""Unit tests for run-level task error summaries."""

from tracker.utils.task_error_summary import summarize_task_errors


def test_task_error_summary_returns_distinct_groups() -> None:
    """All-task-error finalization should surface each distinct current failure.

    Test cases:
    - Similar messages produce one frequency-ordered representative per group.
    - Identical messages produce one representative.
    """
    dominant_summary = summarize_task_errors(
        {
            "pytorch-model-cli": "Modal sandbox sb-101 failed during setup: provider is not implemented",
            "write-compressor": "Modal sandbox sb-202 failed while setting up: provider is not implemented",
            "kv-store-grpc": "Modal sandbox sb-303 could not complete setup: provider is not implemented",
            "torch-tensor-parallelism": "Agent timed out after 600 seconds",
            "largest-eigenval": "Required output artifact was missing",
        }
    )

    assert dominant_summary == (
        "No tasks were completed successfully. 3 distinct task errors:\n"
        "- 3/5 tasks: Modal sandbox sb-101 failed during setup: provider is not implemented\n"
        "- 1/5 tasks: Required output artifact was missing\n"
        "- 1/5 tasks: Agent timed out after 600 seconds"
    )

    identical_summary = summarize_task_errors({f"task-{index}": "Network connection timed out" for index in range(5)})

    assert identical_summary == (
        "No tasks were completed successfully. 1 distinct task error:\n- 5/5 tasks: Network connection timed out"
    )
