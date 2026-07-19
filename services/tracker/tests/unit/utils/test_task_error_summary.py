"""Unit tests for run-level task error summaries.

Run: pytest tests/unit/utils/test_task_error_summary.py
"""

from tracker.utils.task_error_summary import summarize_task_errors


def test_task_error_summary_returns_distinct_groups() -> None:
    """All-task-error finalization should surface each distinct current failure.

    Test cases:
    - Similar messages produce one frequency-ordered representative per group.
    - Chained similarities form one group without counting any task twice.
    - Identical messages produce one representative.
    """

    # Separate error families remain distinct and frequency ordered.
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
        "No tasks were completed successfully. 3 distinct errors:\n"
        "- 3/5 tasks: Modal sandbox sb-101 failed during setup: provider is not implemented\n"
        "- 1/5 tasks: Required output artifact was missing\n"
        "- 1/5 tasks: Agent timed out after 600 seconds"
    )

    # Chained similarities form one connected component.
    chained_summary = summarize_task_errors(
        {
            "task-a": "API key rejected during model setup",
            "task-b": "API key timed out during model setup",
            "task-c": "Network timed out during model setup",
        }
    )

    assert chained_summary == (
        "No tasks were completed successfully. 1 distinct error:\n- 3/3 tasks: API key rejected during model setup"
    )

    # Identical errors collapse into one group.
    identical_summary = summarize_task_errors({f"task-{index}": "Network connection timed out" for index in range(5)})

    assert identical_summary == (
        "No tasks were completed successfully. 1 distinct error:\n- 5/5 tasks: Network connection timed out"
    )
