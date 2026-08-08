"""Tests for tracker session and repository composition."""

from unittest.mock import Mock

from sqlmodel import Session

from tracker.database.transaction import TrackerTransaction


def test_open_transaction_owns_and_closes_its_session() -> None:
    session = Mock(spec=Session)

    with TrackerTransaction.open(lambda: session) as transaction:
        assert transaction.session is session
        assert transaction.run_control._benchmarks is transaction.benchmarks  # type: ignore[attr-defined]
        assert transaction.run_control._tasks is transaction.tasks  # type: ignore[attr-defined]
        transaction.commit()

    session.commit.assert_called_once_with()
    session.close.assert_called_once_with()


def test_repository_accessors_are_lazy_and_cached() -> None:
    session = Mock(spec=Session)

    transaction = TrackerTransaction.from_session(session)

    assert "benchmarks" not in transaction.__dict__
    assert transaction.tasks is transaction.tasks
    assert transaction.executor_control is transaction.executor_control
    assert "benchmarks" not in transaction.__dict__
    assert transaction.run_control._benchmarks is transaction.benchmarks  # type: ignore[attr-defined]
    assert transaction.run_control._tasks is transaction.tasks  # type: ignore[attr-defined]


def test_from_session_does_not_close_supplied_session() -> None:
    session = Mock(spec=Session)

    with TrackerTransaction.from_session(session) as transaction:
        transaction.rollback()

    session.rollback.assert_called_once_with()
    session.close.assert_not_called()
