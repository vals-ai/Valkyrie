"""Tests for run webhook interval validation.

Run: uv run pytest tests/unit/cli/run/test_intervals.py
"""

import click
import pytest

from valkyrie.cli.run.start import validate_intervals


class TestIntervalValidation:
    """Webhook interval normalization and rejection behavior."""

    @pytest.mark.parametrize(
        "intervals",
        [
            (5,),
            (100,),
            (25, 50, 100),
        ],
    )
    def test_accepts_valid_intervals(self, intervals: tuple[int, ...]) -> None:
        """Valid boundary and multi-value intervals must preserve their order.

        Test cases:
        - Minimum and maximum values are accepted.
        - Three ordered intervals are returned as a list.
        """
        assert validate_intervals(intervals) == list(intervals)

    @pytest.mark.parametrize(
        ("intervals", "error_match"),
        [
            ((10, 20, 30, 40), "Maximum of 3"),
            ((3,), "out of range"),
            ((105,), "out of range"),
            ((23,), "divisible by 5"),
        ],
    )
    def test_rejects_invalid_intervals(self, intervals: tuple[int, ...], error_match: str) -> None:
        """Invalid counts, ranges, and increments must fail before a run starts.

        Test cases:
        - More than three intervals are rejected.
        - Out-of-range and non-five-minute values are rejected.
        """
        with pytest.raises(click.UsageError, match=error_match):
            validate_intervals(intervals)
