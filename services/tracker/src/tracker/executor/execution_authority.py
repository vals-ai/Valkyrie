"""Compatibility exports for executor execution authority values.

Database authority locking lives in ``TaskExecutionRepository``. Keeping this
module as a value-object compatibility surface avoids SQL-bearing executor
helpers in repository modules.
"""

from tracker.execution_authority import ExecutionAuthority

__all__ = ["ExecutionAuthority"]
