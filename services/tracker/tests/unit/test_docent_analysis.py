"""Unit tests for invoke_analyzer status-transition behavior.

Run: uv run pytest tests/unit/test_docent_analysis.py
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlmodel import Session

from tracker.database.models import Benchmark, BenchmarkStatus, DocentReadingStatus
from tracker.docent_analysis import invoke_analyzer
from tracker.types import AWSCredentials
from tracker.utils import catch_errors_during_cleanup


@pytest.fixture(autouse=True)
def patch_engine(monkeypatch: pytest.MonkeyPatch, database_session: Session) -> None:
    """Point analyzer sessions at the in-memory test database."""
    monkeypatch.setattr("tracker.docent_analysis.engine", database_session.bind)


class TestInvokeAnalyzer:
    """Docent analyzer status transitions and result URLs."""

    def test_invoke_analyzer_success_sets_done_and_url(
        self,
        monkeypatch: pytest.MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
        aws_credentials: AWSCredentials,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        lambda_response = {"reading_plan_url": "https://x.test/r/123", "ingested": 5}

        def invoke_lambda_success(*_args: object, **_kwargs: object) -> dict[str, str | int]:
            return lambda_response

        monkeypatch.setattr("tracker.docent_analysis.invoke_lambda", invoke_lambda_success)

        result = invoke_analyzer(
            benchmark_id=example_benchmark_object.id,
            lambda_function="analysis-foo",
            payload={"benchmark_id": "x"},
            aws=aws_credentials,
        )

        database_session.refresh(example_benchmark_object)
        assert example_benchmark_object.docent_reading_status == DocentReadingStatus.DONE
        assert example_benchmark_object.docent_reading_url == "https://x.test/r/123"
        assert result == lambda_response

    def test_invoke_analyzer_failure_sets_error_and_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
        aws_credentials: AWSCredentials,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        def boom(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("lambda exploded")

        monkeypatch.setattr("tracker.docent_analysis.invoke_lambda", boom)

        with pytest.raises(RuntimeError, match="lambda exploded"):
            invoke_analyzer(
                benchmark_id=example_benchmark_object.id,
                lambda_function="analysis-foo",
                payload={"benchmark_id": "x"},
                aws=aws_credentials,
            )

        database_session.refresh(example_benchmark_object)
        assert example_benchmark_object.docent_reading_status == DocentReadingStatus.ERROR
        assert example_benchmark_object.docent_reading_url is None

    def test_invoke_analyzer_no_url_still_marks_done(
        self,
        monkeypatch: pytest.MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
        aws_credentials: AWSCredentials,
    ) -> None:
        """Lambda returned successfully but no reading_plan_url — still DONE; URL remains untouched."""
        database_session.add(example_benchmark_object)
        database_session.commit()

        def invoke_lambda_without_url(*_args: object, **_kwargs: object) -> dict[str, int]:
            return {"ingested": 0}

        monkeypatch.setattr("tracker.docent_analysis.invoke_lambda", invoke_lambda_without_url)

        invoke_analyzer(
            benchmark_id=example_benchmark_object.id,
            lambda_function="analysis-foo",
            payload={},
            aws=aws_credentials,
        )

        database_session.refresh(example_benchmark_object)
        assert example_benchmark_object.docent_reading_status == DocentReadingStatus.DONE
        assert example_benchmark_object.docent_reading_url is None


def test_cleanup_sweeps_running_docent_status_to_error(
    database_session: Session,
    example_benchmark_object: Benchmark,
    executor_authority: Any,
) -> None:
    """A benchmark stuck at IN_PROGRESS with docent_reading_status=RUNNING is
    swept to ERROR (both the benchmark itself and the analyzer status).
    """
    from tests.utils import TEST_ORG_ID
    from tracker.database.models import Org

    example_benchmark_object.status = BenchmarkStatus.IN_PROGRESS
    example_benchmark_object.docent_reading_status = DocentReadingStatus.RUNNING
    database_session.add(example_benchmark_object)
    database_session.commit()

    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None
    authority = executor_authority(example_benchmark_object, session=database_session)

    catch_errors_during_cleanup(
        example_benchmark_object.id,
        database_session,
        org,
        authority=authority,
    )

    database_session.refresh(example_benchmark_object)
    assert example_benchmark_object.docent_reading_status == DocentReadingStatus.ERROR
    assert example_benchmark_object.status == BenchmarkStatus.ERROR
