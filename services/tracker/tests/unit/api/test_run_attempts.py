"""Run with `uv run pytest tests/unit/api/test_run_attempts.py`."""

from datetime import datetime
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session

from tests.factories import make_benchmark, make_error_result, make_evaluation_result, make_task
from tests.utils import TEST_ORG_ID
from tracker.api.run_attempts import router
from tracker.auth import get_current_org
from tracker.database.models import (
    AgentCausedExitReason,
    ErrorResult,
    EvaluationResult,
    Org,
    TaskAttempt,
    TaskStatus,
)
from tracker.database.session import get_session

_UTC = ZoneInfo("UTC")
_APP = FastAPI()
_APP.include_router(router)
_CLIENT = TestClient(_APP)
_ORG = Org(id=TEST_ORG_ID, name="default")


def _created_at(hour: int) -> datetime:
    return datetime(2026, 7, 22, hour, tzinfo=_UTC)


def _use_database(database_session: Session) -> None:
    def get_test_session():
        yield database_session

    _APP.dependency_overrides[get_session] = get_test_session
    _APP.dependency_overrides[get_current_org] = lambda: _ORG


def test_run_attempts_openapi_is_paginated_and_discriminated() -> None:
    schema = _APP.openapi()
    operation = schema["paths"]["/benchmarks/{benchmark_id}/attempts"]["get"]
    attempts = schema["components"]["schemas"]["RunAttemptsResponse"]["properties"]["attempts"]["items"]

    assert attempts["discriminator"] == {
        "propertyName": "kind",
        "mapping": {
            "error": "#/components/schemas/RunErrorTaskAttempt",
            "evaluation": "#/components/schemas/RunEvaluationTaskAttempt",
            "execution": "#/components/schemas/RunExecutionTaskAttempt",
        },
    }
    assert {item["$ref"] for item in attempts["oneOf"]} == {
        "#/components/schemas/RunErrorTaskAttempt",
        "#/components/schemas/RunEvaluationTaskAttempt",
        "#/components/schemas/RunExecutionTaskAttempt",
    }
    assert "result" not in schema["components"]["schemas"]["RunEvaluationTaskAttempt"]["properties"]
    assert (
        next(parameter for parameter in operation["parameters"] if parameter["name"] == "limit")["schema"]["maximum"]
        == 500
    )
    assert (
        "maximum"
        not in next(parameter for parameter in operation["parameters"] if parameter["name"] == "offset")["schema"]
    )


def test_attempt_result_indexes_match_history_queries() -> None:
    expected_columns = ["org_id", "task", "created_at DESC", "id DESC"]
    indexes = {
        "ix_evaluationresult_org_task_created_at_id": EvaluationResult.__table__.indexes,
        "ix_errorresult_org_task_created_at_id": ErrorResult.__table__.indexes,
    }

    for name, model_indexes in indexes.items():
        index = next(index for index in model_indexes if index.name == name)
        assert [str(expression).rsplit(".", 1)[-1] for expression in index.expressions] == expected_columns


def test_run_attempts_page_all_tasks_and_preserve_repeated_errors(database_session: Session) -> None:
    """The run feed is globally paginated while attempt numbers remain per task."""
    _use_database(database_session)
    benchmark = make_benchmark()
    first_task = make_task(benchmark, "first", status=TaskStatus.ERROR)
    second_task = make_task(benchmark, "second", status=TaskStatus.FINISHED)
    database_session.add_all([benchmark, first_task, second_task])
    database_session.flush()

    first_error = make_error_result(first_task, "repeated failure", _created_at(12))
    repeated_error = make_error_result(first_task, "repeated failure", _created_at(13))
    evaluation = make_evaluation_result(
        first_task,
        "first-evaluation",
        {"score": 1},
        _created_at(14),
        exit_reason=AgentCausedExitReason.TIMEOUT,
    )
    second_evaluation = make_evaluation_result(second_task, "second-evaluation", {"score": 0}, _created_at(15))
    first_error.id = UUID("00000000-0000-0000-0000-000000000001")
    repeated_error.id = UUID("00000000-0000-0000-0000-000000000002")
    repeated_error.attempt_id = "a2"
    evaluation.id = UUID("00000000-0000-0000-0000-000000000003")
    evaluation.attempt_id = "a3"
    second_evaluation.id = UUID("00000000-0000-0000-0000-000000000004")
    second_evaluation.attempt_id = "a4"
    database_session.add_all([first_error, repeated_error, evaluation, second_evaluation])
    database_session.commit()

    first_response = _CLIENT.get(f"/benchmarks/{benchmark.id}/attempts?limit=3")
    second_response = _CLIENT.get(f"/benchmarks/{benchmark.id}/attempts?limit=3&offset=3")

    assert first_response.status_code == 200, first_response.text
    assert second_response.status_code == 200, second_response.text
    first_page: dict[str, Any] = first_response.json()
    second_page: dict[str, Any] = second_response.json()
    assert first_page["total_count"] == second_page["total_count"] == 4
    assert first_page["attempts"] == [
        {
            "kind": "evaluation",
            "id": str(second_evaluation.id),
            "attempt_id": "a4",
            "created_at": "2026-07-22T15:00:00+00:00",
            "instance_id": "second-evaluation",
            "agent_caused_exit_reason": None,
            "task_id": "second",
            "attempt_number": 1,
            "status": "FINISHED",
        },
        {
            "kind": "evaluation",
            "id": str(evaluation.id),
            "attempt_id": "a3",
            "created_at": "2026-07-22T14:00:00+00:00",
            "instance_id": "first-evaluation",
            "agent_caused_exit_reason": "TIMEOUT",
            "task_id": "first",
            "attempt_number": 3,
            "status": "FINISHED",
        },
        {
            "kind": "error",
            "id": str(repeated_error.id),
            "attempt_id": "a2",
            "created_at": "2026-07-22T13:00:00+00:00",
            "error_message": "repeated failure",
            "error_message_truncated": False,
            "error_fingerprint": sha256(b"repeated failure").hexdigest(),
            "task_id": "first",
            "attempt_number": 2,
            "status": "ERROR",
        },
    ]
    assert second_page["attempts"] == [
        {
            "kind": "error",
            "id": str(first_error.id),
            "attempt_id": None,
            "created_at": "2026-07-22T12:00:00+00:00",
            "error_message": "repeated failure",
            "error_message_truncated": False,
            "error_fingerprint": sha256(b"repeated failure").hexdigest(),
            "task_id": "first",
            "attempt_number": 1,
            "status": "ERROR",
        }
    ]


def test_run_attempts_bound_long_error_and_fingerprint_full_message(database_session: Session) -> None:
    _use_database(database_session)
    benchmark = make_benchmark()
    task = make_task(benchmark, "long-error", status=TaskStatus.ERROR)
    long_message = "x" * 4_000 + "full-message-tail"
    database_session.add_all([benchmark, task])
    database_session.flush()
    database_session.add(make_error_result(task, long_message, _created_at(12)))
    database_session.commit()

    response = _CLIENT.get(f"/benchmarks/{benchmark.id}/attempts")

    assert response.status_code == 200, response.text
    attempt = response.json()["attempts"][0]
    assert attempt["error_message"] == "x" * 4_000
    assert attempt["error_message_truncated"] is True
    assert attempt["error_fingerprint"] == sha256(long_message.encode()).hexdigest()


def test_run_attempts_include_stopped_execution_without_outcome(
    database_session: Session,
) -> None:
    _use_database(database_session)
    benchmark = make_benchmark()
    task = make_task(
        benchmark,
        "stopped",
        status=TaskStatus.STOPPED,
        started_at=_created_at(16),
    )
    attempt = TaskAttempt(
        org_id=task.org_id,
        task=task.id,
        attempt_id="a5",
        started_at=_created_at(15),
        sandbox_provider="daytona",
        sandbox_instance_id="deleted-sandbox",
    )
    database_session.add_all([benchmark, task, attempt])
    database_session.commit()

    response = _CLIENT.get(f"/benchmarks/{benchmark.id}/attempts")

    assert response.status_code == 200, response.text
    assert response.json() == {
        "attempts": [
            {
                "kind": "execution",
                "id": str(attempt.id),
                "attempt_id": "a5",
                "created_at": "2026-07-22T15:00:00+00:00",
                "status": "STOPPED",
                "instance_id": "deleted-sandbox",
                "task_id": "stopped",
                "attempt_number": 1,
            }
        ],
        "total_count": 1,
    }


def test_run_attempts_isolate_run_org_and_result_org(database_session: Session) -> None:
    """Only outcomes owned by the authenticated org and exact run are visible."""
    _use_database(database_session)
    benchmark = make_benchmark()
    task = make_task(benchmark, "target", status=TaskStatus.ERROR)
    other_benchmark = make_benchmark()
    other_run_task = make_task(other_benchmark, "target", status=TaskStatus.ERROR)
    other_org = Org(id=uuid4(), name="other")
    foreign_benchmark = make_benchmark(org_id=other_org.id)
    foreign_task = make_task(foreign_benchmark, "target", status=TaskStatus.ERROR)
    database_session.add_all(
        [benchmark, task, other_benchmark, other_run_task, other_org, foreign_benchmark, foreign_task]
    )
    database_session.flush()

    target = make_error_result(task, "target", _created_at(12))
    wrong_run = make_error_result(other_run_task, "wrong run", _created_at(13))
    wrong_org = make_error_result(foreign_task, "wrong org", _created_at(14))
    wrong_result_org = make_error_result(task, "wrong result org", _created_at(15))
    wrong_result_org.org_id = other_org.id
    database_session.add_all([target, wrong_run, wrong_org, wrong_result_org])
    database_session.commit()

    response = _CLIENT.get(f"/benchmarks/{benchmark.id}/attempts")
    foreign_response = _CLIENT.get(f"/benchmarks/{foreign_benchmark.id}/attempts")

    assert response.status_code == 200, response.text
    assert response.json()["attempts"] == [
        {
            "kind": "error",
            "id": str(target.id),
            "attempt_id": None,
            "created_at": "2026-07-22T12:00:00+00:00",
            "error_message": "target",
            "error_message_truncated": False,
            "error_fingerprint": sha256(b"target").hexdigest(),
            "task_id": "target",
            "attempt_number": 1,
            "status": "ERROR",
        }
    ]
    assert foreign_response.status_code == 404
