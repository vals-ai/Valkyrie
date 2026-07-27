"""Tests for PostgreSQL scheduler identity.

Run: uv run pytest tests/unit/scheduler/test_store.py
"""

from tracker.scheduler.store import queue_pool_id


class TestQueuePoolId:
    """Stable provider-pool identifiers."""

    def test_hashes_provider_pool_deterministically(self) -> None:
        assert queue_pool_id("daytona:organization") == "pool_37e739bd97d514b9ed9df416"
        assert queue_pool_id("daytona:other") != queue_pool_id("daytona:organization")
