"""Maintenance persistence now lives in ``ExecutorControlRepository``.

The sealed release entrypoint and other callers own sessions and transaction
boundaries while invoking the repository methods directly.
"""
