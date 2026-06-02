"""Unit tests for invoke_analyzer status-transition behavior."""

from __future__ import annotations

import pytest
from sqlmodel import Session

from tracker.database.models import Benchmark, BenchmarkStatus, DocentReadingStatus
from tracker.docent_analysis import invoke_analyzer
from tracker.types import AWSCredentials
from tracker.utils import catch_errors_during_cleanup


@pytest.fixture
def aws() -> AWSCredentials:
    return AWSCredentials(
        aws_access_key_id="x",
        aws_secret_access_key="y",
        aws_session_token=None,
        aws_default_region="us-east-1",
    )


@pytest.fixture(autouse=True)
def _patch_engine(monkeypatch: pytest.MonkeyPatch, database_session: Session) -> None:
    """invoke_analyzer opens its own Session via the module-level `engine`; point
    it at the in-memory test DB so its commits land in the same database the
    test fixtures use."""
    monkeypatch.setattr("tracker.docent_analysis.engine", database_session.bind)


def test_invoke_analyzer_success_sets_done_and_url(
    monkeypatch: pytest.MonkeyPatch,
    database_session: Session,
    example_benchmark_object: Benchmark,
    aws: AWSCredentials,
) -> None:
    database_session.add(example_benchmark_object)
    database_session.commit()

    fake_response = {"reading_plan_url": "https://x.test/r/123", "ingested": 5}
    monkeypatch.setattr("tracker.docent_analysis.invoke_lambda", lambda *a, **kw: fake_response)

    result = invoke_analyzer(
        benchmark_id=example_benchmark_object.id,
        lambda_function="analysis-foo",
        payload={"benchmark_id": "x"},
        aws=aws,
    )

    database_session.refresh(example_benchmark_object)
    assert example_benchmark_object.docent_reading_status == DocentReadingStatus.DONE
    assert example_benchmark_object.docent_reading_url == "https://x.test/r/123"
    assert result == fake_response


def test_invoke_analyzer_failure_sets_error_and_raises(
    monkeypatch: pytest.MonkeyPatch,
    database_session: Session,
    example_benchmark_object: Benchmark,
    aws: AWSCredentials,
) -> None:
    database_session.add(example_benchmark_object)
    database_session.commit()

    def boom(*a: object, **kw: object) -> None:
        raise RuntimeError("lambda exploded")

    monkeypatch.setattr("tracker.docent_analysis.invoke_lambda", boom)

    with pytest.raises(RuntimeError, match="lambda exploded"):
        invoke_analyzer(
            benchmark_id=example_benchmark_object.id,
            lambda_function="analysis-foo",
            payload={"benchmark_id": "x"},
            aws=aws,
        )

    database_session.refresh(example_benchmark_object)
    assert example_benchmark_object.docent_reading_status == DocentReadingStatus.ERROR
    assert example_benchmark_object.docent_reading_url is None


def test_invoke_analyzer_no_url_still_marks_done(
    monkeypatch: pytest.MonkeyPatch,
    database_session: Session,
    example_benchmark_object: Benchmark,
    aws: AWSCredentials,
) -> None:
    """Lambda returned successfully but no reading_plan_url — still DONE; URL remains untouched."""
    database_session.add(example_benchmark_object)
    database_session.commit()

    monkeypatch.setattr("tracker.docent_analysis.invoke_lambda", lambda *a, **kw: {"ingested": 0})

    invoke_analyzer(
        benchmark_id=example_benchmark_object.id,
        lambda_function="analysis-foo",
        payload={},
        aws=aws,
    )

    database_session.refresh(example_benchmark_object)
    assert example_benchmark_object.docent_reading_status == DocentReadingStatus.DONE
    assert example_benchmark_object.docent_reading_url is None


def test_cleanup_sweeps_running_docent_status_to_error(
    database_session: Session,
    example_benchmark_object: Benchmark,
) -> None:
    """A benchmark stuck at IN_PROGRESS with docent_reading_status=RUNNING is
    swept to ERROR (both the benchmark itself and the analyzer status)."""
    from tests.conftest import TEST_ORG_ID
    from tracker.database.models import Org

    example_benchmark_object.status = BenchmarkStatus.IN_PROGRESS
    example_benchmark_object.docent_reading_status = DocentReadingStatus.RUNNING
    database_session.add(example_benchmark_object)
    database_session.commit()

    org = database_session.get(Org, TEST_ORG_ID)
    assert org is not None

    catch_errors_during_cleanup(example_benchmark_object.id, database_session, org)

    database_session.refresh(example_benchmark_object)
    assert example_benchmark_object.docent_reading_status == DocentReadingStatus.ERROR
    assert example_benchmark_object.status == BenchmarkStatus.ERROR
