"""Tracker database wiring for live orchestration tests."""

import pytest


@pytest.fixture(autouse=True)
def setup_tracker_database(tracker_database: None) -> None:
    """Connect orchestration code to the test database."""
