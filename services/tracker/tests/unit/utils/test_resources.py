"""Tests for benchmark-service client construction."""

from unittest.mock import patch

from tracker.utils.resources import create_benchmark_service_client


def test_benchmark_service_client_uses_bounded_larger_websocket_limit() -> None:
    with patch("tracker.utils.resources.BenchmarkServiceClient") as client_cls:
        create_benchmark_service_client("https://benchmark-service.example.com")

    client_cls.assert_called_once_with(
        url="https://benchmark-service.example.com",
        headers={},
        max_websocket_message_size=16 * 1024 * 1024,
    )
