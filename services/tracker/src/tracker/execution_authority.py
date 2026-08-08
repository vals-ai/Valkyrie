"""Value objects used to prove executor ownership of a benchmark run."""

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class ExecutionAuthority:
    """Identify the benchmark dispatch currently allowed to persist execution state."""

    benchmark_id: UUID
    dispatch_id: UUID
