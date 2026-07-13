import click
import pytest

from valkyrie.cli.run.start import validate_intervals


class TestIntervalValidation:
    def test_valid_intervals(self) -> None:
        assert validate_intervals((25, 50, 100)) == [25, 50, 100]

    def test_single_interval(self) -> None:
        assert validate_intervals((100,)) == [100]

    def test_rejects_more_than_three(self) -> None:
        with pytest.raises(click.UsageError, match="Maximum of 3"):
            validate_intervals((10, 20, 30, 40))

    def test_rejects_below_range(self) -> None:
        with pytest.raises(click.UsageError, match="out of range"):
            validate_intervals((3,))

    def test_rejects_above_range(self) -> None:
        with pytest.raises(click.UsageError, match="out of range"):
            validate_intervals((105,))

    def test_rejects_not_divisible_by_5(self) -> None:
        with pytest.raises(click.UsageError, match="divisible by 5"):
            validate_intervals((23,))

    def test_minimum_valid_value(self) -> None:
        assert validate_intervals((5,)) == [5]

    def test_maximum_valid_value(self) -> None:
        assert validate_intervals((100,)) == [100]
