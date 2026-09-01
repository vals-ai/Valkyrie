"""Provider-neutral benchmark log capabilities."""

from typing import Protocol


class BenchmarkLogSink(Protocol):
    """Create benchmark log destinations and write task log messages."""

    def create_benchmark(self, benchmark_id: str, *, retention_days: int) -> None:
        """Ensure a benchmark log destination exists."""
        raise NotImplementedError

    def write(self, stream_key: str, message: str) -> None:
        """Write one message to the task stream identified by ``stream_key``."""
        raise NotImplementedError


class BenchmarkLogLocations(Protocol):
    """Provider-native locations for benchmark and task logs."""

    def benchmark_location(self, benchmark_id: str) -> str:
        raise NotImplementedError

    def task_location(self, benchmark_id: str, task_stream_id: str) -> str:
        raise NotImplementedError
