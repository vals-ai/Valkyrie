import pytest
from click.testing import CliRunner


@pytest.fixture
def cli_runner() -> CliRunner:
    """Provide an isolated Click command runner."""
    return CliRunner()
