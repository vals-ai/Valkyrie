"""Exercise executor v1 contracts and claim races against disposable PostgreSQL.

Run: uv run pytest tests/integration/local/database/test_dispatch_api.py
Requests use literal v1 payloads so client and server schema changes cannot mask regressions.
"""

from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Barrier
import json
import asyncio
import os
import socket
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, Mock
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine
from pydantic import SecretStr
from sqlmodel import Session, col, select
from benchmark_service import ImageSource, Resources, Sandbox, SandboxProvider
from benchmark_service.client import BenchmarkServiceClient
from benchmark_service.sandbox import DaytonaProviderConfig
from benchmark_service.schemas import RetrieveTaskResponse

from tests.factories import make_benchmark, make_task
from tracker.aws.runtime import AWSResources
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    DocentReadingStatus,
    ErrorResult,
    EvaluationResult,
    ExecutorDispatch,
    ExecutorDispatchAccess,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    ExecutorRunReceipt,
    ExecutorPoolReservation,
    FinalEvaluation,
    ExecutorTaskAttempt,
    ExecutorTaskReceipt,
    Org,
    Task,
    TaskStatus,
    TaskBreakdown,
)
from tracker.database.session import get_session
from tracker.executor.dispatch_api import create_dispatch_access
from tracker.executor.release_control import (
    create_executor_dispatch,
    pin_benchmark_to_release,
    register_release,
    promote_release,
    QueuePoolBusyError,
)
from tracker.executor.dispatch_control import admit_start_dispatch, admit_recovery_dispatch
from tracker.scheduler.store import queue_pool_lock
from tracker.executor_api.v1.router import router
from tracker.executor_api.transport import ExecutorTransport
from tracker.executor_api.v1.client import ExecutorClient
from tracker.executor_api.v1.schemas import ClaimRequest
from tracker.executor_api.v1.task_schemas import BuildTask, RunTask, EvaluateTask, CompleteTask
from tracker.executor_api.v1.finalization_schemas import CompleteRun
from tracker.executor.task_persistence import ApiTaskPersistence
from tracker.executor.checkpoints import CheckpointCallback
from tracker.executor.execution_authority import ExecutionAuthority
from tracker.exceptions import SandboxSetupError
from tracker.runtime.services import RuntimeServices
from tracker.types import StartBenchmarkRequest
from tracker.utils.task_execution import process_task


@dataclass(frozen=True)
class DispatchFixture:
    dispatch_id: UUID
    benchmark_id: UUID
    task_id: UUID
    token: str
    claim: dict[str, str]

    @property
    def path(self) -> str:
        return f"/internal/executor/v1/dispatches/{self.dispatch_id}"

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    @property
    def request(self) -> dict[str, str]:
        return {"claimant_id": self.claim["claimant_id"]}


@pytest.fixture
def dispatch(postgres_session: Session) -> DispatchFixture:
    org = Org(name="dispatch-api-test")
    postgres_session.add(org)
    postgres_session.flush()
    benchmark = make_benchmark(org_id=org.id)
    release = ExecutorRelease(
        id="dispatch-api-release",
        artifact_uri="s3://artifacts/executor.pex",
        artifact_digest="a" * 64,
        protocol_version="2",
        readiness_verified=True,
    )
    register_release(postgres_session, release)
    pin_benchmark_to_release(benchmark, release)
    postgres_session.add(benchmark)
    postgres_session.flush()
    task = make_task(benchmark, "task-0", status=TaskStatus.IN_PROGRESS)
    postgres_session.add(task)
    invocation = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=[task.task_id]
    )
    postgres_session.add(invocation)
    postgres_session.flush()
    token = create_dispatch_access(postgres_session, invocation)
    postgres_session.commit()

    return DispatchFixture(
        dispatch_id=invocation.id,
        benchmark_id=benchmark.id,
        task_id=task.id,
        token=token,
        claim={
            "claimant_id": str(uuid4()),
            "benchmark_id": str(benchmark.id),
            "executor_release_id": release.id,
            "executor_artifact_uri": release.artifact_uri,
            "executor_artifact_digest": release.artifact_digest,
            "executor_protocol_version": release.protocol_version,
        },
    )


@pytest.fixture
def app(postgres_engine: Engine) -> FastAPI:
    application = FastAPI()
    application.include_router(router)

    def session_dependency() -> Generator[Session, None, None]:
        with Session(postgres_engine, expire_on_commit=False) as session:
            yield session

    application.dependency_overrides[get_session] = session_dependency

    return application


@pytest.fixture
def client(app: FastAPI) -> Generator[TestClient, None, None]:
    with TestClient(app) as test_client:
        yield test_client


@pytest.mark.parametrize(
    "operation",
    [
        "claim",
        "authority",
        "heartbeat",
        "finish",
        "fail",
        "run/initialize",
        "run/state",
        "run/finalization",
        "run/finalize",
    ],
)
def test_dispatch_credentials_are_required(client: TestClient, dispatch: DispatchFixture, operation: str) -> None:
    """Reject missing and unrelated credentials independently of user API auth settings.

    Test cases:
    - Every operation rejects a missing bearer token.
    - Every operation rejects a token belonging to no dispatch.
    """
    payload = dispatch.claim if operation == "claim" else dispatch.request
    if operation == "fail":
        payload = {**payload, "error_message": "test failure"}

    assert client.post(f"{dispatch.path}/{operation}", json=payload).status_code == 401
    response = client.post(
        f"{dispatch.path}/{operation}", json=payload, headers={"Authorization": "Bearer unrelated-token"}
    )

    assert response.status_code == 401


def test_claim_replays_only_for_the_same_process(
    client: TestClient, dispatch: DispatchFixture, postgres_session: Session
) -> None:
    """Preserve one claim across a lost response without accepting a second process.

    Test cases:
    - Identical retries return the original lease, without extending it.
    - Redelivery with a fresh claimant cannot execute the dispatch.
    - The credential digest, never the plaintext, is persisted.
    """
    first = client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers)
    replay = client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers)
    duplicate = client.post(
        f"{dispatch.path}/claim", json={**dispatch.claim, "claimant_id": str(uuid4())}, headers=dispatch.headers
    )

    assert first.status_code == 200, first.json()
    first_lease = first.json()
    replay_lease = replay.json()
    first_time = datetime.fromisoformat(first_lease.pop("server_time"))
    replay_time = datetime.fromisoformat(replay_lease.pop("server_time"))
    assert replay_lease == first_lease
    assert first_time <= replay_time < datetime.fromisoformat(first_lease["lease_expires_at"])
    assert duplicate.status_code == 409
    assert first.json()["lease_expires_at"].endswith("Z")

    for operation in ("heartbeat", "finish", "fail"):
        payload = {"claimant_id": str(uuid4())}
        if operation == "fail":
            payload["error_message"] = "Wrong process"
        response = client.post(f"{dispatch.path}/{operation}", json=payload, headers=dispatch.headers)
        assert response.status_code == 409

    access = postgres_session.get(ExecutorDispatchAccess, dispatch.dispatch_id)
    assert access is not None
    assert access.token_digest != dispatch.token
    assert str(access.claimant_id) == dispatch.claim["claimant_id"]


def test_concurrent_claimants_have_exactly_one_winner(app: FastAPI, dispatch: DispatchFixture) -> None:
    """Fence concurrent claim requests through separate HTTP and database sessions.

    Test cases:
    - Exactly one of two different processes acquires the dispatch.
    - The winner can replay while the losing claimant remains rejected.
    """
    barrier = Barrier(2)
    claimants = [str(uuid4()), str(uuid4())]

    def claim(claimant: str) -> int:
        with TestClient(app) as concurrent_client:
            barrier.wait(timeout=10)
            return concurrent_client.post(
                f"{dispatch.path}/claim", json={**dispatch.claim, "claimant_id": claimant}, headers=dispatch.headers
            ).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        statuses = list(pool.map(claim, claimants))

    assert sorted(statuses) == [200, 409]
    with TestClient(app) as test_client:
        for claimant, status in zip(claimants, statuses, strict=True):
            response = test_client.post(
                f"{dispatch.path}/claim", json={**dispatch.claim, "claimant_id": claimant}, headers=dispatch.headers
            )
            assert response.status_code == status


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("benchmark_id", "00000000-0000-0000-0000-000000000001"),
        ("executor_release_id", "another-release"),
        ("executor_artifact_uri", "s3://artifacts/another.pex"),
        ("executor_artifact_digest", "b" * 64),
        ("executor_protocol_version", "99"),
    ],
)
def test_claim_checks_the_entire_release_pin(
    client: TestClient, dispatch: DispatchFixture, field: str, replacement: str
) -> None:
    """Reject queue payloads that do not match the dispatch's immutable identity.

    Test cases:
    - Any mismatched release field rejects the claim without consuming it.
    - The original payload can still claim the dispatch afterward.
    """
    response = client.post(
        f"{dispatch.path}/claim", json={**dispatch.claim, field: replacement}, headers=dispatch.headers
    )

    assert response.status_code == 409
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200


@pytest.mark.parametrize("expired", [False, True])
def test_heartbeat_cannot_restore_revoked_authority(
    client: TestClient, dispatch: DispatchFixture, postgres_session: Session, expired: bool
) -> None:
    """Accept healthy heartbeats but reject expired or explicitly stopped execution.

    Test cases:
    - An active process renews its lease and retains authority.
    - Expiry or a user stop revokes heartbeat, claim replay, and terminal writes.
    """
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    assert client.post(f"{dispatch.path}/heartbeat", json=dispatch.request, headers=dispatch.headers).status_code == 200
    assert client.post(f"{dispatch.path}/authority", json=dispatch.request, headers=dispatch.headers).json() == {
        "current": True
    }
    if expired:
        invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
        assert invocation is not None
        invocation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        postgres_session.add(invocation)
    else:
        benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
        assert benchmark is not None
        benchmark.status = BenchmarkStatus.STOPPED
        postgres_session.add(benchmark)
    postgres_session.commit()

    assert client.post(f"{dispatch.path}/authority", json=dispatch.request, headers=dispatch.headers).json() == {
        "current": False
    }
    for operation in ("heartbeat", "finish", "claim"):
        payload = dispatch.claim if operation == "claim" else dispatch.request
        response = client.post(f"{dispatch.path}/{operation}", json=payload, headers=dispatch.headers)
        assert response.status_code == 409


@pytest.mark.parametrize("operation", ["finish", "fail"])
def test_terminal_receipt_survives_client_restart_and_run_recovery(
    app: FastAPI, client: TestClient, dispatch: DispatchFixture, postgres_session: Session, operation: str
) -> None:
    """Replay committed terminal results without writing to a later run execution.

    Test cases:
    - A fresh API client reads the same receipt after the benchmark resumes.
    - Conflicting terminal calls and subsequent heartbeats remain forbidden.
    - Replay never terminalizes the resumed benchmark or duplicates error rows.
    """
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    payload = dispatch.request
    if operation == "fail":
        payload = {**payload, "error_message": "Executor test failure"}
    first = client.post(f"{dispatch.path}/{operation}", json=payload, headers=dispatch.headers)
    assert first.status_code == 200

    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    benchmark.status = BenchmarkStatus.IN_PROGRESS
    benchmark.finished_at = None
    postgres_session.add(benchmark)
    postgres_session.commit()

    with TestClient(app) as restarted_client:
        replay = restarted_client.post(f"{dispatch.path}/{operation}", json=payload, headers=dispatch.headers)
    assert replay.status_code == 200
    assert replay.json() == first.json()
    assert client.post(f"{dispatch.path}/heartbeat", json=dispatch.request, headers=dispatch.headers).status_code == 409
    conflict = client.post(
        f"{dispatch.path}/fail",
        json={**dispatch.request, "error_message": "A different failure"},
        headers=dispatch.headers,
    )
    assert conflict.status_code == 409

    postgres_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    errors = postgres_session.exec(select(ErrorResult).where(ErrorResult.task == dispatch.task_id)).all()
    assert len(errors) == (1 if operation == "fail" else 0)


def test_failure_preserves_tasks_assigned_to_a_live_sibling(
    client: TestClient, dispatch: DispatchFixture, postgres_session: Session
) -> None:
    """Use persisted task assignments to avoid failing another dispatch's work.

    Test cases:
    - A failed invocation records its terminal receipt.
    - A live sibling retains its shared task and the benchmark stays in progress.
    """
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    release = postgres_session.get(ExecutorRelease, dispatch.claim["executor_release_id"])
    task = postgres_session.get(Task, dispatch.task_id)
    assert release is not None and task is not None
    sibling = create_executor_dispatch(
        dispatch.benchmark_id, release, ExecutorDispatchKind.RESUME, dispatch_id=uuid4(), task_ids=[task.task_id]
    )
    postgres_session.add(sibling)
    postgres_session.commit()

    response = client.post(
        f"{dispatch.path}/fail", json={**dispatch.request, "error_message": "test failure"}, headers=dispatch.headers
    )

    assert response.status_code == 200
    postgres_session.refresh(task)
    postgres_session.refresh(sibling)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert task.status == TaskStatus.IN_PROGRESS
    assert sibling.status == ExecutorDispatchStatus.QUEUED


def test_finish_waits_for_cancellation_cleanup(
    client: TestClient, dispatch: DispatchFixture, postgres_session: Session
) -> None:
    """Prevent a successful finish from bypassing an in-progress user stop.

    Test cases:
    - A stopping benchmark rejects finish but permits a cleanup heartbeat.
    - Failure cleanup terminalizes the stopping benchmark without restoring execution.
    """
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    benchmark.status = BenchmarkStatus.STOPPING
    postgres_session.add(benchmark)
    postgres_session.commit()

    assert client.post(f"{dispatch.path}/finish", json=dispatch.request, headers=dispatch.headers).status_code == 409
    assert client.post(f"{dispatch.path}/heartbeat", json=dispatch.request, headers=dispatch.headers).status_code == 200
    response = client.post(
        f"{dispatch.path}/fail", json={**dispatch.request, "error_message": "Cancelled"}, headers=dispatch.headers
    )

    assert response.status_code == 200
    postgres_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.STOPPED


def test_failure_refuses_unknown_task_assignments(
    client: TestClient, dispatch: DispatchFixture, postgres_session: Session
) -> None:
    """Never guess which tasks an invocation may terminalize.

    Test cases:
    - Missing legacy assignments reject failure cleanup without changing any task.
    """
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    assert invocation is not None
    invocation.assigned_task_ids = None
    postgres_session.add(invocation)
    postgres_session.commit()

    response = client.post(
        f"{dispatch.path}/fail", json={**dispatch.request, "error_message": "test failure"}, headers=dispatch.headers
    )

    assert response.status_code == 409
    postgres_session.refresh(invocation)
    assert invocation.status == ExecutorDispatchStatus.RUNNING
    task = postgres_session.get(Task, dispatch.task_id)
    assert task is not None
    assert task.status == TaskStatus.IN_PROGRESS


@pytest.fixture
def assigned_run(postgres_session: Session, dispatch: DispatchFixture) -> list[str]:
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    assert benchmark is not None and invocation is not None
    benchmark.started_at = datetime(2026, 1, 1, tzinfo=UTC)
    benchmark.started_by_email = "executor-test@example.com"
    benchmark.aws_managed = True
    benchmark.arguments = benchmark.arguments.model_copy(
        update={
            "concurrency": 3,
            "queue_pool_id": "pool-1",
            "properties": AWSResources("us-west-2", "run-artifacts", "run-logs", 7),
        }
    )
    evaluating = make_task(
        benchmark, "evaluating", status=TaskStatus.EVALUATING, started_at=datetime(2026, 1, 2, tzinfo=UTC)
    )
    evaluating.eval_resume_state = {"instance_id": "existing-instance", "outputs": ["kept"]}
    sibling_task = make_task(benchmark, "sibling-task", status=TaskStatus.FINISHED)
    task_ids = ["new-task", "evaluating", "task-0"]
    invocation.assigned_task_ids = task_ids
    postgres_session.add_all([benchmark, invocation, evaluating, sibling_task])
    foreign_org = Org(name="unrelated-run-owner")
    postgres_session.add(foreign_org)
    postgres_session.flush()
    foreign_benchmark = make_benchmark(org_id=foreign_org.id, name="unrelated-run")
    postgres_session.add(foreign_benchmark)
    postgres_session.flush()
    foreign_task = make_task(foreign_benchmark, "new-task", status=TaskStatus.FINISHED)
    foreign_task.eval_resume_state = {"private": "unrelated-checkpoint"}
    postgres_session.add(foreign_task)
    postgres_session.commit()

    return task_ids


def test_run_initialization_preserves_existing_attempts(
    client: TestClient, dispatch: DispatchFixture, assigned_run: list[str], postgres_session: Session
) -> None:
    """Initialize only missing assigned rows and return a stable v1 snapshot.

    Test cases:
    - Replaying initialization preserves row IDs, attempt timestamps, status, and evaluation checkpoints.
    - Requested order survives database ordering; run-wide counts include sibling tasks.
    - Normal polling omits checkpoint contents without modifying the saved checkpoint.
    """
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    request = {**dispatch.request, "task_ids": assigned_run, "include_eval_resume_state": True}
    initialized = client.post(f"{dispatch.path}/run/initialize", json=request, headers=dispatch.headers)
    replay = client.post(f"{dispatch.path}/run/initialize", json=request, headers=dispatch.headers)

    assert initialized.status_code == 200, initialized.text
    assert replay.json() == initialized.json()
    state = initialized.json()
    assert state["current"] is True
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    assert state["run"] == {
        "benchmark_id": str(dispatch.benchmark_id),
        "org_id": str(benchmark.org_id),
        "org_name": "dispatch-api-test",
        "benchmark_name": "swebench",
        "agent_name": "a",
        "started_by_email": "executor-test@example.com",
        "model": None,
        "started_at": "2026-01-01T00:00:00Z",
        "status": "IN_PROGRESS",
        "aws_managed": True,
        "concurrency": 3,
        "queue_pool_id": "pool-1",
        "resources": {
            "region": "us-west-2",
            "s3_bucket": "run-artifacts",
            "log_group": "run-logs",
            "log_retention_days": 7,
        },
    }
    assert [task["task_id"] for task in state["tasks"]] == assigned_run
    assert [task["status"] for task in state["tasks"]] == ["PENDING", "EVALUATING", "IN_PROGRESS"]
    assert state["tasks"][1]["started_at"] == "2026-01-02T00:00:00Z"
    assert state["tasks"][1]["eval_resume_state"] == {"instance_id": "existing-instance", "outputs": ["kept"]}
    assert state["tasks"][0]["eval_resume_state"] is None
    assert state["task_counts"] == {
        "PENDING": 1,
        "BUILDING": 0,
        "IN_PROGRESS": 1,
        "EVALUATING": 1,
        "STOPPED": 0,
        "FINISHED": 1,
        "ERROR": 0,
    }
    polled = client.post(
        f"{dispatch.path}/run/state", json={**dispatch.request, "task_ids": assigned_run}, headers=dispatch.headers
    )
    assert polled.status_code == 200
    assert all(task["eval_resume_state"] is None for task in polled.json()["tasks"])
    checkpoint = postgres_session.exec(
        select(Task).where(Task.benchmark == dispatch.benchmark_id, Task.task_id == "evaluating")
    ).one()
    assert checkpoint.eval_resume_state == {"instance_id": "existing-instance", "outputs": ["kept"]}


@pytest.mark.parametrize("operation", ["run/initialize", "run/state"])
def test_run_api_rejects_unassigned_tasks_and_unclaimed_processes(
    client: TestClient, dispatch: DispatchFixture, assigned_run: list[str], postgres_session: Session, operation: str
) -> None:
    """Scope task access to the persisted assignment, even within the same run.

    Test cases:
    - The credential alone does not replace a successful process claim.
    - Requests for a sibling's task or an invented task fail without creating rows.
    - A wrong claimant and a missing legacy assignment cannot access run state.
    """
    path = f"{dispatch.path}/{operation}"
    request = {**dispatch.request, "task_ids": assigned_run}
    assert client.post(path, json=request, headers=dispatch.headers).status_code == 409
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    assert client.post(path, json={**request, "claimant_id": str(uuid4())}, headers=dispatch.headers).status_code == 409
    for task_id in ("sibling-task", "invented-task"):
        response = client.post(path, json={**request, "task_ids": ["new-task", task_id]}, headers=dispatch.headers)
        assert response.status_code == 409
    assert (
        postgres_session.exec(
            select(Task).where(Task.benchmark == dispatch.benchmark_id, Task.task_id == "new-task")
        ).first()
        is None
    )

    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    assert invocation is not None
    invocation.assigned_task_ids = None
    postgres_session.add(invocation)
    postgres_session.commit()
    assert client.post(path, json={**request, "task_ids": []}, headers=dispatch.headers).status_code == 409


@pytest.mark.parametrize("revocation", ["stopping", "stopped", "expired", "failed", "finished"])
def test_run_state_reports_revocation_without_creating_tasks(
    client: TestClient, dispatch: DispatchFixture, assigned_run: list[str], postgres_session: Session, revocation: str
) -> None:
    """Allow an old claimant to observe cancellation without authorizing new writes.

    Test cases:
    - Stopped or expired claims remain readable with current=false.
    - Stopping or terminal runs reject initialization even when their dispatch lease remains live.
    - A missing assigned row is reported as uninitialized, never fabricated by a read.
    """
    assert assigned_run
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert invocation is not None and benchmark is not None
    if revocation == "expired":
        invocation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    else:
        benchmark.status = {
            "stopping": BenchmarkStatus.STOPPING,
            "stopped": BenchmarkStatus.STOPPED,
            "failed": BenchmarkStatus.ERROR,
            "finished": BenchmarkStatus.FINISHED,
        }[revocation]
    postgres_session.add_all([invocation, benchmark])
    postgres_session.commit()

    response = client.post(
        f"{dispatch.path}/run/state", json={**dispatch.request, "task_ids": ["task-0"]}, headers=dispatch.headers
    )
    assert response.status_code == 200, response.text
    assert response.json()["current"] is (revocation not in ("stopped", "expired"))
    assert response.json()["run"]["status"] == benchmark.status.value
    for operation in ("run/state", "run/initialize"):
        response = client.post(
            f"{dispatch.path}/{operation}",
            json={**dispatch.request, "task_ids": ["new-task"]},
            headers=dispatch.headers,
        )
        assert response.status_code == 409
    assert (
        postgres_session.exec(
            select(Task).where(Task.benchmark == dispatch.benchmark_id, Task.task_id == "new-task")
        ).first()
        is None
    )


def test_concurrent_initialization_creates_one_attempt_per_task(
    app: FastAPI, client: TestClient, dispatch: DispatchFixture, assigned_run: list[str], postgres_session: Session
) -> None:
    """Serialize overlapping initialization requests without duplicating or resetting tasks.

    Test cases:
    - Concurrent sessions return the same persisted row and attempt identities.
    - Exactly one new row exists for each assigned dataset task.
    """
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    barrier = Barrier(2)

    def initialize() -> httpx.Response:
        with TestClient(app) as concurrent_client:
            barrier.wait(timeout=5)
            return concurrent_client.post(
                f"{dispatch.path}/run/initialize",
                json={**dispatch.request, "task_ids": assigned_run},
                headers=dispatch.headers,
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(initialize) for _ in range(2)]
        responses = [future.result(timeout=10) for future in futures]
    assert [response.status_code for response in responses] == [200, 200]
    assert responses[0].json() == responses[1].json()
    rows = postgres_session.exec(select(Task).where(Task.benchmark == dispatch.benchmark_id)).all()
    assert sorted(task.task_id for task in rows) == sorted([*assigned_run, "sibling-task"])


def test_initialization_cannot_adopt_a_newer_task_attempt(
    client: TestClient, dispatch: DispatchFixture, assigned_run: list[str], postgres_session: Session
) -> None:
    """Reject stale initialization while allowing the monitor to observe a retry.

    Test cases:
    - A newer attempt timestamp invalidates initialization under the old dispatch.
    - Rejection rolls back other missing rows from the same batch.
    - State reads return the new timestamp so the old executor can cancel its stale task.
    """
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    task = postgres_session.get(Task, dispatch.task_id)
    assert invocation is not None and task is not None
    task.started_at = invocation.created_at + timedelta(seconds=1)
    postgres_session.add(task)
    postgres_session.commit()

    response = client.post(
        f"{dispatch.path}/run/initialize", json={**dispatch.request, "task_ids": assigned_run}, headers=dispatch.headers
    )
    assert response.status_code == 409
    assert (
        postgres_session.exec(
            select(Task).where(Task.benchmark == dispatch.benchmark_id, Task.task_id == "new-task")
        ).first()
        is None
    )
    polled = client.post(
        f"{dispatch.path}/run/state", json={**dispatch.request, "task_ids": ["task-0"]}, headers=dispatch.headers
    )
    assert polled.status_code == 200
    assert datetime.fromisoformat(polled.json()["tasks"][0]["started_at"]).replace(
        tzinfo=None
    ) == task.started_at.replace(tzinfo=None)


def test_initialized_attempt_can_be_cleaned_up_without_touching_siblings(
    client: TestClient, dispatch: DispatchFixture, assigned_run: list[str], postgres_session: Session
) -> None:
    """Keep newly created attempt timestamps compatible with dispatch failure cleanup.

    Test cases:
    - Failure after initialization terminalizes the new assigned attempt.
    - The run's unassigned finished sibling remains unchanged.
    """
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    initialized = client.post(
        f"{dispatch.path}/run/initialize", json={**dispatch.request, "task_ids": assigned_run}, headers=dispatch.headers
    )
    assert initialized.status_code == 200
    failed = client.post(
        f"{dispatch.path}/fail", json={**dispatch.request, "error_message": "executor failed"}, headers=dispatch.headers
    )
    assert failed.status_code == 200
    statuses = dict(
        postgres_session.exec(select(Task.task_id, Task.status).where(Task.benchmark == dispatch.benchmark_id)).all()
    )
    assert statuses == {
        "new-task": TaskStatus.ERROR,
        "evaluating": TaskStatus.ERROR,
        "task-0": TaskStatus.ERROR,
        "sibling-task": TaskStatus.FINISHED,
    }


@dataclass(frozen=True)
class TaskFixture:
    id: UUID
    path: str
    request: dict[str, str]


@pytest.fixture
def task_attempt(
    client: TestClient, dispatch: DispatchFixture, assigned_run: list[str], postgres_session: Session
) -> TaskFixture:
    assert "new-task" in assigned_run
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    benchmark.arguments = benchmark.arguments.model_copy(update={"queue_pool_id": None, "priority": None})
    postgres_session.add(benchmark)
    postgres_session.commit()
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    initialized = client.post(
        f"{dispatch.path}/run/initialize", json={**dispatch.request, "task_ids": ["new-task"]}, headers=dispatch.headers
    )
    assert initialized.status_code == 200, initialized.text
    task = initialized.json()["tasks"][0]

    return TaskFixture(
        id=UUID(task["id"]),
        path=f"{dispatch.path}/tasks/{task['id']}",
        request={**dispatch.request, "expected_started_at": task["started_at"]},
    )


@pytest.fixture
def owned_task(client: TestClient, dispatch: DispatchFixture, task_attempt: TaskFixture) -> TaskFixture:
    response = client.post(
        f"{task_attempt.path}/claim",
        json={**task_attempt.request, "command_id": str(uuid4())},
        headers=dispatch.headers,
    )
    assert response.status_code == 200, response.text
    assert response.json()["revision"] == 0

    return task_attempt


def _write_request(task: TaskFixture, revision: int, mutation: dict[str, object]) -> dict[str, object]:
    return {**task.request, "command_id": str(uuid4()), "expected_revision": revision, "mutation": mutation}


def test_task_writes_preserve_order_and_replay_committed_results(
    client: TestClient, dispatch: DispatchFixture, owned_task: TaskFixture, postgres_session: Session
) -> None:
    """Commit task progress, checkpoints, timings, and results exactly once per command.

    Test cases:
    - Every lifecycle command advances its write revision and replays without another mutation.
    - An older checkpoint replay cannot overwrite a later checkpoint.
    - Completion retries after dispatch expiry return the original receipt without duplicating results.
    """
    commands: list[dict[str, object]] = [
        {"operation": "build"},
        {"operation": "run"},
        {"operation": "evaluate", "sandbox_build_duration": 1.5, "agent_run_duration": 2.5},
        {"operation": "checkpoint", "checkpoint": {"cursor": "first"}},
        {"operation": "checkpoint", "checkpoint": {"cursor": "second"}},
        {
            "operation": "complete",
            "result": {"score": 1, "passed": True},
            "instance_id": "sandbox-1",
            "exit_reason": "TIMEOUT",
            "evaluation_run_duration": 3.5,
            "sandbox_run_duration": 7.5,
        },
    ]
    requests: list[dict[str, object]] = []
    for revision, mutation in enumerate(commands):
        request = _write_request(owned_task, revision, mutation)
        requests.append(request)
        response = client.post(f"{owned_task.path}/write", json=request, headers=dispatch.headers)
        replay = client.post(f"{owned_task.path}/write", json=request, headers=dispatch.headers)
        assert response.status_code == 200, response.text
        assert response.json() == {
            "command_id": request["command_id"],
            "task_id": str(owned_task.id),
            "revision": revision + 1,
        }
        assert replay.json() == response.json()
    assert client.post(f"{owned_task.path}/write", json=requests[3], headers=dispatch.headers).status_code == 200

    task = postgres_session.get(Task, owned_task.id)
    assert task is not None and task.status == TaskStatus.FINISHED and task.finished_at is not None
    assert task.eval_resume_state == {"cursor": "second"}
    breakdown = postgres_session.get(TaskBreakdown, task.task_breakdown)
    assert breakdown is not None
    assert (
        breakdown.sandbox_build_duration,
        breakdown.agent_run_duration,
        breakdown.evaluation_run_duration,
        breakdown.sandbox_run_duration,
    ) == (1.5, 2.5, 3.5, 7.5)
    result = postgres_session.exec(select(EvaluationResult).where(EvaluationResult.task == task.id)).one()
    assert result.result == {"score": 1, "passed": True}
    assert result.instance_id == "sandbox-1"
    assert result.agent_caused_exit_reason is not None and result.agent_caused_exit_reason.value == "TIMEOUT"

    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    assert invocation is not None
    invocation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    postgres_session.add(invocation)
    postgres_session.commit()
    assert client.post(f"{owned_task.path}/write", json=requests[-1], headers=dispatch.headers).status_code == 200
    changed = {**requests[-1], "mutation": {"operation": "complete", "result": {"score": 0}}}
    assert client.post(f"{owned_task.path}/write", json=changed, headers=dispatch.headers).status_code == 409
    assert len(postgres_session.exec(select(EvaluationResult).where(EvaluationResult.task == task.id)).all()) == 1


def test_task_retry_history_and_terminal_errors_are_atomic(
    client: TestClient, dispatch: DispatchFixture, owned_task: TaskFixture, postgres_session: Session
) -> None:
    """Preserve recovery history without treating a scheduled retry as terminal.

    Test cases:
    - Retry history is recorded once while the task stays runnable.
    - Returning to pending and starting again preserves the attempt identity.
    - A final error records provenance and terminal status in the same transaction.
    """
    error = {
        "error_message": "provider unavailable",
        "producer": "sandbox_provider",
        "operation_name": "setup",
        "error_type": "SandboxSetupError",
        "cause_code": "transient",
    }
    mutations: list[dict[str, object]] = [
        {"operation": "build"},
        {"operation": "retry", "failed_attempt_number": 1, **error},
        {"operation": "pending"},
        {"operation": "build"},
        {"operation": "fail", **error},
    ]
    for revision, mutation in enumerate(mutations):
        request = _write_request(owned_task, revision, mutation)
        for _ in range(2):
            response = client.post(f"{owned_task.path}/write", json=request, headers=dispatch.headers)
            assert response.status_code == 200, response.text
        if revision == 1:
            task = postgres_session.get(Task, owned_task.id)
            assert task is not None and task.status == TaskStatus.BUILDING
    postgres_session.expire_all()
    task = postgres_session.get(Task, owned_task.id)
    assert task is not None and task.status == TaskStatus.ERROR and task.finished_at is not None
    assert task.started_at.replace(tzinfo=UTC) == datetime.fromisoformat(owned_task.request["expected_started_at"])
    errors = postgres_session.exec(
        select(ErrorResult).where(ErrorResult.task == task.id).order_by(col(ErrorResult.created_at))
    ).all()
    assert len(errors) == 2
    assert [(row.retry_scheduled, row.failed_attempt_number) for row in errors] == [(True, 1), (False, None)]
    assert all(
        (row.producer, row.operation, row.error_type, row.cause_code)
        == ("sandbox_provider", "setup", "SandboxSetupError", "transient")
        for row in errors
    )


@pytest.mark.parametrize(
    "invalid",
    ["unclaimed", "expired", "stopped", "new-attempt", "claimant", "assignment", "task", "revision", "organization"],
)
def test_task_write_fences_reject_stale_or_unassigned_work(
    client: TestClient, dispatch: DispatchFixture, owned_task: TaskFixture, postgres_session: Session, invalid: str
) -> None:
    """Reject unauthorized writes before touching task state or storing a receipt.

    Test cases:
    - Task ownership, live dispatch authority, assignment, and exact attempt time are mandatory.
    - An outdated revision cannot overwrite a later task mutation.
    """
    request = _write_request(owned_task, 0, {"operation": "build"})
    path = f"{owned_task.path}/write"
    task = postgres_session.get(Task, owned_task.id)
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    owner = postgres_session.get(ExecutorTaskAttempt, owned_task.id)
    assert task is not None and invocation is not None and benchmark is not None and owner is not None
    if invalid == "unclaimed":
        postgres_session.delete(owner)
    elif invalid == "expired":
        invocation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        postgres_session.add(invocation)
    elif invalid == "stopped":
        benchmark.status = BenchmarkStatus.STOPPED
        postgres_session.add(benchmark)
    elif invalid == "new-attempt":
        task.started_at = invocation.created_at + timedelta(seconds=1)
        postgres_session.add(task)
    elif invalid == "claimant":
        request["claimant_id"] = str(uuid4())
    elif invalid == "assignment":
        invocation.assigned_task_ids = ["task-0"]
        postgres_session.add(invocation)
    elif invalid == "task":
        path = f"{dispatch.path}/tasks/{uuid4()}/write"
    elif invalid == "organization":
        other_org = Org(name="other-task-org")
        postgres_session.add(other_org)
        postgres_session.flush()
        task.org_id = other_org.id
        postgres_session.add(task)
    else:
        request["expected_revision"] = 3
    postgres_session.commit()
    response = client.post(path, json=request, headers=dispatch.headers)
    assert response.status_code == 409, response.text
    postgres_session.refresh(task)
    assert task.status == TaskStatus.PENDING
    assert postgres_session.get(ExecutorTaskReceipt, (dispatch.dispatch_id, UUID(str(request["command_id"])))) is None


@pytest.mark.parametrize("invalid", ["expired", "stopped", "organization", "building"])
def test_task_claim_rejects_unavailable_attempts(
    client: TestClient, dispatch: DispatchFixture, task_attempt: TaskFixture, postgres_session: Session, invalid: str
) -> None:
    """Reject task ownership when the run, dispatch, or task cannot authorize execution.

    Test cases:
    - Expired dispatches and stopped runs cannot claim pending work.
    - A task in another organization or already building cannot acquire an owner.
    """
    task = postgres_session.get(Task, task_attempt.id)
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert task is not None and invocation is not None and benchmark is not None
    if invalid == "expired":
        invocation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        postgres_session.add(invocation)
    elif invalid == "stopped":
        benchmark.status = BenchmarkStatus.STOPPED
        postgres_session.add(benchmark)
    elif invalid == "organization":
        other_org = Org(name="other-task-org")
        postgres_session.add(other_org)
        postgres_session.flush()
        task.org_id = other_org.id
        postgres_session.add(task)
    else:
        task.status = TaskStatus.BUILDING
        postgres_session.add(task)
    postgres_session.commit()
    request = {**task_attempt.request, "command_id": str(uuid4())}

    response = client.post(f"{task_attempt.path}/claim", json=request, headers=dispatch.headers)

    assert response.status_code == 409, response.text
    assert postgres_session.get(ExecutorTaskAttempt, task_attempt.id) is None
    assert postgres_session.get(ExecutorTaskReceipt, (dispatch.dispatch_id, UUID(request["command_id"]))) is None


def test_concurrent_task_writes_have_one_winner(
    app: FastAPI, client: TestClient, dispatch: DispatchFixture, owned_task: TaskFixture, postgres_session: Session
) -> None:
    """Serialize different commands racing for the same task revision.

    Test cases:
    - Only one of build and stop can commit at revision zero.
    - Replaying the winner leaves the committed task revision unchanged.
    """
    barrier = Barrier(2)
    requests = [_write_request(owned_task, 0, {"operation": operation}) for operation in ("build", "stop")]

    def write(request: dict[str, object]) -> httpx.Response:
        with TestClient(app) as concurrent_client:
            barrier.wait(timeout=5)
            return concurrent_client.post(f"{owned_task.path}/write", json=request, headers=dispatch.headers)

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(write, requests))
    assert sorted(response.status_code for response in responses) == [200, 409]
    winning_index = next(index for index, response in enumerate(responses) if response.status_code == 200)
    assert (
        client.post(f"{owned_task.path}/write", json=requests[winning_index], headers=dispatch.headers).json()
        == responses[winning_index].json()
    )
    task = postgres_session.get(Task, owned_task.id)
    owner = postgres_session.get(ExecutorTaskAttempt, owned_task.id)
    assert task is not None and owner is not None and owner.revision == 1
    assert task.status == (TaskStatus.BUILDING if winning_index == 0 else TaskStatus.STOPPED)


def test_stopping_run_accepts_cleanup_but_not_new_work(
    client: TestClient, dispatch: DispatchFixture, owned_task: TaskFixture, postgres_session: Session
) -> None:
    """Preserve evaluation output during stop without starting more execution.

    Test cases:
    - A stopping run rejects build, retry, and pending commands without advancing revisions.
    - Checkpoints and final results can still commit for an evaluating attempt.
    """
    for revision, operation in enumerate(("build", "run", "evaluate")):
        response = client.post(
            f"{owned_task.path}/write",
            json=_write_request(owned_task, revision, {"operation": operation}),
            headers=dispatch.headers,
        )
        assert response.status_code == 200, response.text

    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    benchmark.status = BenchmarkStatus.STOPPING
    postgres_session.add(benchmark)
    postgres_session.commit()

    rejected: list[dict[str, object]] = [
        {"operation": "build"},
        {"operation": "pending"},
        {
            "operation": "retry",
            "failed_attempt_number": 1,
            "error_message": "retry",
            "producer": "executor",
            "operation_name": "evaluate",
            "error_type": "EvaluationError",
        },
    ]
    for mutation in rejected:
        response = client.post(
            f"{owned_task.path}/write", json=_write_request(owned_task, 3, mutation), headers=dispatch.headers
        )
        assert response.status_code == 409, response.text

    checkpoint = _write_request(owned_task, 3, {"operation": "checkpoint", "checkpoint": {"step": 2}})
    assert client.post(f"{owned_task.path}/write", json=checkpoint, headers=dispatch.headers).status_code == 200
    stale_checkpoint = _write_request(owned_task, 3, {"operation": "checkpoint", "checkpoint": {"step": 1}})
    assert client.post(f"{owned_task.path}/write", json=stale_checkpoint, headers=dispatch.headers).status_code == 409
    result = _write_request(owned_task, 4, {"operation": "complete", "result": {"score": 1}})
    assert client.post(f"{owned_task.path}/write", json=result, headers=dispatch.headers).status_code == 200

    postgres_session.expire_all()
    task = postgres_session.get(Task, owned_task.id)
    owner = postgres_session.get(ExecutorTaskAttempt, owned_task.id)
    assert task is not None and task.status == TaskStatus.FINISHED and task.eval_resume_state == {"step": 2}
    assert owner is not None and owner.revision == 5
    assert not postgres_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).all()


@pytest.mark.parametrize("operation", ["run", "evaluate", "checkpoint", "complete"])
def test_invalid_task_transition_leaves_no_side_effects(
    client: TestClient, dispatch: DispatchFixture, owned_task: TaskFixture, postgres_session: Session, operation: str
) -> None:
    """Reject out-of-order execution commands without saving a successful receipt.

    Test cases:
    - A pending task cannot run, evaluate, checkpoint, or finish before building.
    - Rejected commands leave its state, revision, result, and timing records unchanged.
    """
    mutation: dict[str, object] = {"operation": operation}
    if operation == "checkpoint":
        mutation["checkpoint"] = {"step": 1}
    elif operation == "complete":
        mutation["result"] = {"score": 1}
    request = _write_request(owned_task, 0, mutation)

    response = client.post(f"{owned_task.path}/write", json=request, headers=dispatch.headers)

    assert response.status_code == 409, response.text
    task = postgres_session.get(Task, owned_task.id)
    owner = postgres_session.get(ExecutorTaskAttempt, owned_task.id)
    assert task is not None and task.status == TaskStatus.PENDING
    assert task.task_breakdown is None and task.eval_resume_state is None
    assert owner is not None and owner.revision == 0
    assert postgres_session.get(ExecutorTaskReceipt, (dispatch.dispatch_id, UUID(str(request["command_id"])))) is None
    assert not postgres_session.exec(select(EvaluationResult).where(EvaluationResult.task == task.id)).all()


@pytest.mark.parametrize("same_attempt", [True, False])
def test_evaluation_claim_requires_dispatch_attempt(
    client: TestClient,
    dispatch: DispatchFixture,
    task_attempt: TaskFixture,
    postgres_session: Session,
    same_attempt: bool,
) -> None:
    """Keep an old evaluation checkpoint from being claimed as a different attempt.

    Test cases:
    - Evaluation resume accepts the exact dispatch attempt timestamp.
    - An older evaluating attempt remains unchanged and unclaimed.
    """
    task = postgres_session.get(Task, task_attempt.id)
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    assert task is not None and invocation is not None
    task.status = TaskStatus.EVALUATING
    task.started_at = invocation.created_at - timedelta(seconds=0 if same_attempt else 1)
    task.eval_resume_state = {"step": 2}
    postgres_session.add(task)
    postgres_session.commit()
    request = {
        **task_attempt.request,
        "command_id": str(uuid4()),
        "expected_started_at": task.started_at.replace(tzinfo=UTC).isoformat(),
    }

    response = client.post(f"{task_attempt.path}/claim", json=request, headers=dispatch.headers)

    assert response.status_code == (200 if same_attempt else 409), response.text
    assert (postgres_session.get(ExecutorTaskAttempt, task.id) is not None) == same_attempt
    postgres_session.refresh(task)
    assert task.eval_resume_state == {"step": 2} and task.status == TaskStatus.EVALUATING


def test_sibling_can_claim_only_after_the_attempt_changes(
    client: TestClient, dispatch: DispatchFixture, owned_task: TaskFixture, postgres_session: Session
) -> None:
    """Prevent sibling dispatches from sharing ownership of one attempt.

    Test cases:
    - A live sibling with the same task assignment cannot claim the existing attempt.
    - An explicitly new attempt can transfer ownership without authorizing the old writer.
    """
    release = postgres_session.get(ExecutorRelease, dispatch.claim["executor_release_id"])
    assert release is not None
    sibling = create_executor_dispatch(
        dispatch.benchmark_id, release, ExecutorDispatchKind.RESUME, dispatch_id=uuid4(), task_ids=["new-task"]
    )
    postgres_session.add(sibling)
    postgres_session.flush()
    token = create_dispatch_access(postgres_session, sibling)
    postgres_session.commit()
    sibling_path = f"/internal/executor/v1/dispatches/{sibling.id}"
    headers = {"Authorization": f"Bearer {token}"}
    claimant_id = str(uuid4())
    assert (
        client.post(
            f"{sibling_path}/claim", json={**dispatch.claim, "claimant_id": claimant_id}, headers=headers
        ).status_code
        == 200
    )
    request = {**owned_task.request, "claimant_id": claimant_id, "command_id": str(uuid4())}
    assert client.post(f"{sibling_path}/tasks/{owned_task.id}/claim", json=request, headers=headers).status_code == 409
    task = postgres_session.get(Task, owned_task.id)
    assert task is not None
    task.started_at = sibling.created_at
    postgres_session.add(task)
    postgres_session.commit()
    request["expected_started_at"] = sibling.created_at.replace(tzinfo=UTC).isoformat()
    response = client.post(f"{sibling_path}/tasks/{owned_task.id}/claim", json=request, headers=headers)
    assert response.status_code == 200, response.text
    assert response.json()["revision"] == 0
    old_write = _write_request(owned_task, 0, {"operation": "build"})
    assert client.post(f"{owned_task.path}/write", json=old_write, headers=dispatch.headers).status_code == 409
    new_write = {**request, "command_id": str(uuid4()), "expected_revision": 0, "mutation": {"operation": "build"}}
    assert (
        client.post(f"{sibling_path}/tasks/{owned_task.id}/write", json=new_write, headers=headers).status_code == 200
    )


@pytest.mark.parametrize("operation", ["claim", "write", "queue/reserve", "queue/release"])
def test_task_commands_require_dispatch_credentials(
    client: TestClient, task_attempt: TaskFixture, operation: str
) -> None:
    """Keep task mutations separate from user-level API authentication.

    Test cases:
    - Missing or unrelated dispatch credentials never authorize task writes.
    """
    request = (
        {**task_attempt.request, "command_id": str(uuid4())}
        if operation == "claim"
        else _write_request(task_attempt, 0, {"operation": "build"})
    )
    for headers in ({}, {"Authorization": "Bearer unrelated"}):
        assert client.post(f"{task_attempt.path}/{operation}", json=request, headers=headers).status_code == 401


@pytest.fixture
def queued_task(owned_task: TaskFixture, dispatch: DispatchFixture, postgres_session: Session) -> TaskFixture:
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    original = postgres_session.get(Task, dispatch.task_id)
    assert benchmark is not None and original is not None
    original.status = TaskStatus.FINISHED
    benchmark.aws_managed = False
    benchmark.arguments = benchmark.arguments.model_copy(update={"queue_pool_id": "pool_test", "priority": 2})
    postgres_session.add_all([original, benchmark])
    postgres_session.commit()

    return owned_task


def _reserve_request(task: TaskFixture, revision: int = 0) -> dict[str, object]:
    return {**task.request, "command_id": str(uuid4()), "expected_revision": revision}


def _release_request(task: TaskFixture, reservation_id: object) -> dict[str, object]:
    return {**task.request, "command_id": str(uuid4()), "reservation_id": reservation_id}


def test_queue_creation_requires_a_persistent_reservation(
    app: FastAPI, client: TestClient, dispatch: DispatchFixture, queued_task: TaskFixture, postgres_session: Session
) -> None:
    """Keep creation serialized across API instances and release-response retries.

    Test cases:
    - A queued task cannot build without a reservation.
    - A fresh API instance reads the same reservation and revision.
    - Build and run use the reserved attempt; releasing does not mutate task state.
    - Old reserve and release commands cannot acquire or delete a newer reservation.
    """
    path = queued_task.path
    rejected = client.post(
        f"{path}/write", json=_write_request(queued_task, 0, {"operation": "build"}), headers=dispatch.headers
    )
    assert rejected.status_code == 409
    request = _reserve_request(queued_task)
    reserved = client.post(f"{path}/queue/reserve", json=request, headers=dispatch.headers)
    assert reserved.status_code == 200 and reserved.json()["reserved"], reserved.text
    assert reserved.json()["revision"] == 1

    restarted_app = FastAPI()
    restarted_app.include_router(router)
    restarted_app.dependency_overrides[get_session] = app.dependency_overrides[get_session]
    with TestClient(restarted_app) as restarted_client:
        response = restarted_client.post(f"{path}/queue/reserve", json=request, headers=dispatch.headers)
        assert response.json() == reserved.json()
        waiting = restarted_client.post(
            f"{path}/queue/reserve", json=_reserve_request(queued_task, 1), headers=dispatch.headers
        )
        assert waiting.status_code == 200 and not waiting.json()["reserved"]

    for revision, operation in ((1, "build"), (2, "run")):
        response = client.post(
            f"{path}/write",
            json=_write_request(queued_task, revision, {"operation": operation}),
            headers=dispatch.headers,
        )
        assert response.status_code == 200, response.text
    release = _release_request(queued_task, request["command_id"])
    response = client.post(f"{path}/queue/release", json=release, headers=dispatch.headers)
    assert response.status_code == 200 and response.json()["released"]
    assert postgres_session.get(ExecutorPoolReservation, "pool_test") is None

    pending = _write_request(queued_task, 3, {"operation": "pending"})
    assert client.post(f"{path}/write", json=pending, headers=dispatch.headers).status_code == 200
    next_request = _reserve_request(queued_task, 4)
    assert client.post(f"{path}/queue/reserve", json=next_request, headers=dispatch.headers).json()["reserved"]
    assert client.post(f"{path}/queue/release", json=release, headers=dispatch.headers).json() == response.json()
    assert client.post(f"{path}/queue/reserve", json=request, headers=dispatch.headers).status_code == 409
    reservation = postgres_session.get(ExecutorPoolReservation, "pool_test")
    assert reservation is not None and str(reservation.reservation_id) == next_request["command_id"]


def test_expired_queue_reservation_waits_for_owner_cleanup(
    client: TestClient, dispatch: DispatchFixture, queued_task: TaskFixture, postgres_session: Session
) -> None:
    """Do not infer a settled provider operation from an expired dispatch lease.

    Test cases:
    - Expiry revokes creation authority but leaves the persistent reservation intact.
    - The original claimant can confirm cleanup without restoring task write authority.
    """
    request = _reserve_request(queued_task)
    assert client.post(f"{queued_task.path}/queue/reserve", json=request, headers=dispatch.headers).json()["reserved"]
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    assert invocation is not None
    invocation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    postgres_session.add(invocation)
    postgres_session.commit()

    assert client.post(f"{queued_task.path}/queue/reserve", json=request, headers=dispatch.headers).status_code == 409
    build = _write_request(queued_task, 1, {"operation": "build"})
    assert client.post(f"{queued_task.path}/write", json=build, headers=dispatch.headers).status_code == 409
    reservation = postgres_session.get(ExecutorPoolReservation, "pool_test")
    assert reservation is not None
    release = _release_request(queued_task, request["command_id"])
    assert client.post(f"{queued_task.path}/queue/release", json=release, headers=dispatch.headers).status_code == 200
    postgres_session.expire_all()
    assert postgres_session.exec(select(ExecutorPoolReservation)).first() is None
    task = postgres_session.get(Task, queued_task.id)
    owner = postgres_session.get(ExecutorTaskAttempt, queued_task.id)
    assert task is not None and task.status == TaskStatus.PENDING
    assert owner is not None and owner.revision == 1


def test_queue_reservation_waits_for_legacy_pool_work(
    client: TestClient, dispatch: DispatchFixture, queued_task: TaskFixture, postgres_session: Session
) -> None:
    """Do not mix API reservations with a legacy executor that ignores them.

    Test cases:
    - An active legacy dispatch in the provider pool prevents new reservations.
    - The legacy run is left untouched; reservations become available after it finishes.
    """
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    release = postgres_session.get(ExecutorRelease, dispatch.claim["executor_release_id"])
    assert benchmark is not None and release is not None
    legacy = make_benchmark(org_id=benchmark.org_id)
    legacy.arguments = legacy.arguments.model_copy(update={"queue_pool_id": "pool_test", "priority": 3})
    postgres_session.add(legacy)
    postgres_session.flush()
    invocation = create_executor_dispatch(
        legacy.id, release, ExecutorDispatchKind.START, dispatch_id=uuid4(), task_ids=[]
    )
    postgres_session.add(invocation)
    postgres_session.commit()
    request = _reserve_request(queued_task)

    response = client.post(f"{queued_task.path}/queue/reserve", json=request, headers=dispatch.headers)

    assert response.status_code == 200 and not response.json()["reserved"]
    postgres_session.refresh(legacy)
    assert legacy.status == BenchmarkStatus.IN_PROGRESS
    legacy.status = BenchmarkStatus.FINISHED
    invocation.status = ExecutorDispatchStatus.FINISHED
    postgres_session.add_all([legacy, invocation])
    postgres_session.commit()
    assert client.post(f"{queued_task.path}/queue/reserve", json=request, headers=dispatch.headers).json()["reserved"]


async def test_queue_reservation_waits_for_legacy_creation_lock(
    client: TestClient, dispatch: DispatchFixture, queued_task: TaskFixture, postgres_engine: Engine
) -> None:
    """Respect a legacy provider call even after its database run becomes terminal.

    Test cases:
    - A legacy session advisory lock blocks the API's reservation transaction.
    - Releasing that lock permits reservation without retaining an API database connection.
    """
    request = _reserve_request(queued_task)
    async with queue_pool_lock(postgres_engine, "pool_test") as acquired:
        assert acquired
        response = await asyncio.to_thread(
            client.post, f"{queued_task.path}/queue/reserve", json=request, headers=dispatch.headers
        )
        assert response.status_code == 200 and not response.json()["reserved"]

    response = await asyncio.to_thread(
        client.post, f"{queued_task.path}/queue/reserve", json=request, headers=dispatch.headers
    )
    assert response.status_code == 200 and response.json()["reserved"]


@pytest.mark.parametrize("operation", ["start", "resume"])
def test_queue_reservation_fences_new_legacy_admission(
    client: TestClient, dispatch: DispatchFixture, queued_task: TaskFixture, postgres_session: Session, operation: str
) -> None:
    """Prevent a legacy admission from racing into an already reserved provider pool.

    Test cases:
    - New starts and additive resumes reject before changing existing execution state.
    - Releasing the reservation permits admission to continue.
    """
    promote_release(postgres_session, dispatch.claim["executor_release_id"])
    postgres_session.commit()
    request = _reserve_request(queued_task)
    assert client.post(f"{queued_task.path}/queue/reserve", json=request, headers=dispatch.headers).json()["reserved"]
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    target = make_benchmark(org_id=benchmark.org_id) if operation == "start" else benchmark
    target.arguments = target.arguments.model_copy(update={"queue_pool_id": "pool_test", "priority": 2})

    def admit() -> ExecutorDispatch:
        if operation == "start":
            return admit_start_dispatch(postgres_session, benchmark=target, dispatch_id=uuid4(), task_ids=[])
        return admit_recovery_dispatch(
            postgres_session,
            benchmark=target,
            pre_action_status=BenchmarkStatus.IN_PROGRESS,
            dispatch_id=uuid4(),
            kind=ExecutorDispatchKind.RESUME,
            task_ids=[],
        )

    with pytest.raises(QueuePoolBusyError):
        admit()
    postgres_session.rollback()
    postgres_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert (
        client.post(
            f"{queued_task.path}/queue/release",
            json=_release_request(queued_task, request["command_id"]),
            headers=dispatch.headers,
        ).status_code
        == 200
    )
    admitted = admit()
    postgres_session.commit()
    assert admitted.status == ExecutorDispatchStatus.QUEUED


def test_queue_reservation_racing_admission_has_one_winner(
    app: FastAPI,
    dispatch: DispatchFixture,
    queued_task: TaskFixture,
    postgres_session: Session,
    postgres_engine: Engine,
) -> None:
    """Close the race between checking for legacy work and admitting a new legacy run.

    Test cases:
    - A reservation and same-pool legacy admission cannot both commit.
    - The losing operation leaves the existing run and task intact.
    """
    promote_release(postgres_session, dispatch.claim["executor_release_id"])
    postgres_session.commit()
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    org_id = benchmark.org_id
    barrier = Barrier(2)

    def reserve() -> bool:
        with TestClient(app) as concurrent_client:
            barrier.wait(timeout=5)
            response = concurrent_client.post(
                f"{queued_task.path}/queue/reserve", json=_reserve_request(queued_task), headers=dispatch.headers
            )
            assert response.status_code == 200, response.text
            return bool(response.json()["reserved"])

    def admit() -> bool:
        with Session(postgres_engine) as session:
            target = make_benchmark(org_id=org_id)
            target.arguments = target.arguments.model_copy(update={"queue_pool_id": "pool_test", "priority": 2})
            barrier.wait(timeout=5)
            try:
                admit_start_dispatch(session, benchmark=target, dispatch_id=uuid4(), task_ids=[])
            except QueuePoolBusyError:
                session.rollback()
                return False
            session.commit()
            return True

    with ThreadPoolExecutor(max_workers=2) as pool:
        reservation = pool.submit(reserve)
        admission = pool.submit(admit)
        assert sorted((reservation.result(timeout=10), admission.result(timeout=10))) == [False, True]
    postgres_session.refresh(benchmark)
    task = postgres_session.get(Task, queued_task.id)
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS
    assert task is not None and task.status == TaskStatus.PENDING


@pytest.fixture
def finalizable_run(client: TestClient, dispatch: DispatchFixture, postgres_session: Session) -> str:
    assert client.post(f"{dispatch.path}/claim", json=dispatch.claim, headers=dispatch.headers).status_code == 200
    task = postgres_session.get(Task, dispatch.task_id)
    assert task is not None
    task.status = TaskStatus.FINISHED
    postgres_session.add(task)
    postgres_session.add(EvaluationResult(org_id=task.org_id, task=task.id, result={"score": 0.75}))
    postgres_session.commit()
    response = client.post(f"{dispatch.path}/run/finalization", json=dispatch.request, headers=dispatch.headers)
    assert response.status_code == 200, response.text
    assert response.json()["operation"] == "complete"
    assert response.json()["evaluation_results"] == {"task-0": {"score": 0.75}}
    digest = response.json()["snapshot_digest"]
    assert isinstance(digest, str)

    return digest


def _finalize_request(dispatch: DispatchFixture, snapshot_digest: str) -> dict[str, object]:
    return {
        **dispatch.request,
        "command_id": str(uuid4()),
        "snapshot_digest": snapshot_digest,
        "finalization": {"operation": "complete", "final_score": 0.75, "metadata": {"weight": 1}},
    }


def test_run_finalization_replays_receipt_after_expiry_and_retry(
    client: TestClient, dispatch: DispatchFixture, finalizable_run: str, postgres_session: Session
) -> None:
    """Persist one score and replay its response without changing a later run attempt.

    Test cases:
    - Final score, metadata, status, and receipt commit together.
    - Lease expiry and a later attempt do not duplicate or restore the old score.
    - A changed request cannot reuse the committed command identifier.
    """
    request = _finalize_request(dispatch, finalizable_run)
    response = client.post(f"{dispatch.path}/run/finalize", json=request, headers=dispatch.headers)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "FINISHED" and body["benchmark_id"] == str(dispatch.benchmark_id)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    evaluation = postgres_session.get(FinalEvaluation, UUID(body["final_evaluation_id"]))
    assert benchmark is not None and benchmark.status == BenchmarkStatus.FINISHED and benchmark.finished_at is not None
    assert evaluation is not None and evaluation.final_score == 0.75 and evaluation.properties == {"weight": 1}
    assert client.post(f"{dispatch.path}/finish", json=dispatch.request, headers=dispatch.headers).status_code == 200

    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    task = postgres_session.get(Task, dispatch.task_id)
    assert invocation is not None and task is not None
    invocation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    task.started_at += timedelta(seconds=1)
    task.status = TaskStatus.PENDING
    benchmark.status = BenchmarkStatus.IN_PROGRESS
    benchmark.finished_at = None
    postgres_session.add_all([invocation, task, benchmark])
    postgres_session.delete(evaluation)
    postgres_session.commit()

    replay = client.post(f"{dispatch.path}/run/finalize", json=request, headers=dispatch.headers)
    assert replay.status_code == 200 and replay.json() == body
    changed = {**request, "finalization": {"operation": "complete", "final_score": 0.5}}
    assert client.post(f"{dispatch.path}/run/finalize", json=changed, headers=dispatch.headers).status_code == 409
    postgres_session.refresh(benchmark)
    postgres_session.refresh(task)
    assert benchmark.status == BenchmarkStatus.IN_PROGRESS and task.status == TaskStatus.PENDING
    assert not postgres_session.exec(select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark.id)).all()
    assert len(postgres_session.exec(select(ExecutorRunReceipt)).all()) == 1


@pytest.mark.parametrize("change", ["retry", "new-result", "stop", "expired", "claimant", "new-task", "missing-task"])
def test_run_finalization_rejects_changed_snapshot(
    client: TestClient, dispatch: DispatchFixture, finalizable_run: str, postgres_session: Session, change: str
) -> None:
    """Discard an aggregate calculated against stale task results or lost authority.

    Test cases:
    - Retry, replacement results, stop, and task admission invalidate the snapshot.
    - Expired leases and another claimant cannot commit a final score.
    - Rejection leaves no score or successful finalization receipt.
    """
    request = _finalize_request(dispatch, finalizable_run)
    task = postgres_session.get(Task, dispatch.task_id)
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert task is not None and invocation is not None and benchmark is not None
    if change == "retry":
        task.status = TaskStatus.PENDING
        task.started_at += timedelta(seconds=1)
        postgres_session.add(task)
    elif change == "new-result":
        postgres_session.add(EvaluationResult(org_id=task.org_id, task=task.id, result={"score": 1}))
    elif change == "stop":
        benchmark.status = BenchmarkStatus.STOPPING
        postgres_session.add(benchmark)
    elif change == "expired":
        invocation.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        postgres_session.add(invocation)
    elif change == "claimant":
        request["claimant_id"] = str(uuid4())
    elif change == "new-task":
        postgres_session.add(make_task(benchmark, "new-task", status=TaskStatus.ERROR))
    else:
        invocation.assigned_task_ids = ["task-0", "not-initialized"]
        postgres_session.add(invocation)
    postgres_session.commit()

    response = client.post(f"{dispatch.path}/run/finalize", json=request, headers=dispatch.headers)

    assert response.status_code == 409, response.text
    postgres_session.refresh(benchmark)
    assert benchmark.status in (BenchmarkStatus.IN_PROGRESS, BenchmarkStatus.STOPPING)
    assert not postgres_session.exec(select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark.id)).all()
    assert not postgres_session.exec(select(ExecutorRunReceipt)).all()


@pytest.mark.parametrize("task_status", [TaskStatus.ERROR, TaskStatus.STOPPED])
def test_run_finalization_without_results(
    client: TestClient,
    dispatch: DispatchFixture,
    finalizable_run: str,
    postgres_session: Session,
    task_status: TaskStatus,
) -> None:
    """Finalize failed or stopped tasks without accepting an invented score.

    Test cases:
    - All-error runs expose task errors, commit the summary, and fail unfinished Docent processing.
    - Stopped runs retain STOPPED without adding a final evaluation.
    """
    task = postgres_session.get(Task, dispatch.task_id)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert task is not None and benchmark is not None
    benchmark.docent_reading_status = DocentReadingStatus.RUNNING
    postgres_session.add(benchmark)
    task.status = task_status
    postgres_session.add(task)
    if task_status == TaskStatus.ERROR:
        postgres_session.add(ErrorResult(org_id=task.org_id, task=task.id, error_message="Agent failed"))
    postgres_session.commit()
    response = client.post(f"{dispatch.path}/run/finalization", json=dispatch.request, headers=dispatch.headers)
    assert response.status_code == 200, response.text
    state = response.json()
    assert state["evaluation_results"] == {"task-0": None}
    assert state["snapshot_digest"] != finalizable_run
    assert state["operation"] == ("fail" if task_status == TaskStatus.ERROR else "stop")
    if task_status == TaskStatus.ERROR:
        assert state["task_errors"] == {"task-0": "Agent failed"}

    request = _finalize_request(dispatch, state["snapshot_digest"])
    assert client.post(f"{dispatch.path}/run/finalize", json=request, headers=dispatch.headers).status_code == 409
    request["finalization"] = (
        {"operation": "fail", "error_message": "Every task failed"}
        if task_status == TaskStatus.ERROR
        else {"operation": "stop"}
    )
    for _ in range(2):
        response = client.post(f"{dispatch.path}/run/finalize", json=request, headers=dispatch.headers)
        assert response.status_code == 200, response.text
        assert response.json()["status"] == task_status.value
        assert response.json()["final_evaluation_id"] is None

    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    postgres_session.refresh(benchmark)
    assert benchmark.error_message == ("Every task failed" if task_status == TaskStatus.ERROR else None)
    assert benchmark.docent_reading_status == (
        DocentReadingStatus.ERROR if task_status == TaskStatus.ERROR else DocentReadingStatus.RUNNING
    )
    assert not postgres_session.exec(select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark.id)).all()


def test_concurrent_finalization_commits_one_score(
    app: FastAPI, dispatch: DispatchFixture, finalizable_run: str, postgres_session: Session
) -> None:
    """Serialize competing coordinators at run completion.

    Test cases:
    - Two commands using the same result snapshot cannot both persist scores.
    - Exactly one final evaluation and receipt survive the race.
    """
    barrier = Barrier(2)
    requests = [_finalize_request(dispatch, finalizable_run) for _ in range(2)]

    def finalize(request: dict[str, object]) -> httpx.Response:
        with TestClient(app) as concurrent_client:
            barrier.wait(timeout=5)
            return concurrent_client.post(f"{dispatch.path}/run/finalize", json=request, headers=dispatch.headers)

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(finalize, requests))

    assert sorted(response.status_code for response in responses) == [200, 409]
    assert len(postgres_session.exec(select(FinalEvaluation)).all()) == 1
    assert len(postgres_session.exec(select(ExecutorRunReceipt)).all()) == 1


def test_finalization_waits_for_uninitialized_sibling_work(
    client: TestClient, dispatch: DispatchFixture, finalizable_run: str, postgres_session: Session
) -> None:
    """Do not finish a run before another admitted dispatch initializes its tasks.

    Test cases:
    - A queued sibling with a missing task prevents finalization.
    - Once that task finishes, a fresh snapshot includes its result.
    - Committing the fresh aggregate invalidates the sibling dispatch.
    """
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    release = postgres_session.get(ExecutorRelease, dispatch.claim["executor_release_id"])
    assert benchmark is not None and release is not None
    sibling = create_executor_dispatch(
        benchmark.id, release, ExecutorDispatchKind.RESUME, dispatch_id=uuid4(), task_ids=["later-task"]
    )
    postgres_session.add(sibling)
    postgres_session.commit()

    state = client.post(f"{dispatch.path}/run/finalization", json=dispatch.request, headers=dispatch.headers).json()
    assert state["current"] and state["snapshot_digest"] is None and state["operation"] is None
    assert (
        client.post(
            f"{dispatch.path}/run/finalize", json=_finalize_request(dispatch, finalizable_run), headers=dispatch.headers
        ).status_code
        == 409
    )

    task = make_task(benchmark, "later-task", status=TaskStatus.FINISHED)
    postgres_session.add(task)
    postgres_session.flush()
    postgres_session.add(EvaluationResult(org_id=benchmark.org_id, task=task.id, result={"score": 0.25}))
    postgres_session.commit()
    state = client.post(f"{dispatch.path}/run/finalization", json=dispatch.request, headers=dispatch.headers).json()
    assert state["evaluation_results"] == {"task-0": {"score": 0.75}, "later-task": {"score": 0.25}}
    request = _finalize_request(dispatch, state["snapshot_digest"])
    request["finalization"] = {"operation": "complete", "final_score": 0.5}

    response = client.post(f"{dispatch.path}/run/finalize", json=request, headers=dispatch.headers)

    assert response.status_code == 200, response.text
    postgres_session.refresh(sibling)
    assert sibling.status == ExecutorDispatchStatus.FAILED
    evaluation = postgres_session.exec(select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark.id)).one()
    assert evaluation.final_score == 0.5


def test_stopped_run_replaces_previous_score_atomically(
    client: TestClient, dispatch: DispatchFixture, finalizable_run: str, postgres_session: Session
) -> None:
    """Preserve a partial aggregate when completed tasks coexist with stopped tasks.

    Test cases:
    - A new aggregate replaces the earlier run attempt's final evaluation.
    - The run remains STOPPED and receipt replay does not duplicate the score.
    """
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert benchmark is not None
    previous_score = FinalEvaluation(org_id=benchmark.org_id, benchmark=benchmark.id, final_score=0.1)
    postgres_session.add(previous_score)
    postgres_session.add(make_task(benchmark, "stopped-task", status=TaskStatus.STOPPED))
    postgres_session.commit()
    state = client.post(f"{dispatch.path}/run/finalization", json=dispatch.request, headers=dispatch.headers).json()
    assert state["operation"] == "complete" and state["snapshot_digest"] != finalizable_run
    request = _finalize_request(dispatch, state["snapshot_digest"])

    response = client.post(f"{dispatch.path}/run/finalize", json=request, headers=dispatch.headers)

    assert response.status_code == 200 and response.json()["status"] == "STOPPED", response.text
    assert (
        client.post(f"{dispatch.path}/run/finalize", json=request, headers=dispatch.headers).json() == response.json()
    )
    evaluations = postgres_session.exec(select(FinalEvaluation).where(FinalEvaluation.benchmark == benchmark.id)).all()
    assert len(evaluations) == 1 and evaluations[0].id != previous_score.id
    assert evaluations[0].final_score == 0.75
    postgres_session.refresh(benchmark)
    assert benchmark.status == BenchmarkStatus.STOPPED


class MockLostResponseTransport(httpx.AsyncBaseTransport):
    """Drop a successful response only after the real API has committed it."""

    def __init__(self, app: FastAPI, operation: str, *, mutation: str | None = None) -> None:
        self._transport = httpx.ASGITransport(app)
        self._operation = operation
        self._mutation = mutation
        self.dropped = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self._transport.handle_async_request(request)
        matches_mutation = (
            self._mutation is None or json.loads(request.content).get("mutation", {}).get("operation") == self._mutation
        )
        if (
            request.url.path.endswith(f"/{self._operation}")
            and matches_mutation
            and response.status_code == 200
            and not self.dropped
        ):
            self.dropped = True
            await response.aclose()
            raise httpx.ReadError("Connection lost after server commit", request=request)

        return response

    async def aclose(self) -> None:
        await self._transport.aclose()


class MockPausedResponseTransport(httpx.ASGITransport):
    """Hold one committed response while the executor receives cancellation."""

    def __init__(self, app: FastAPI, operation: str) -> None:
        super().__init__(app)
        self.operation = operation
        self.committed = asyncio.Event()
        self.release = asyncio.Event()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await super().handle_async_request(request)
        if request.url.path.endswith(self.operation) and not self.committed.is_set():
            assert response.status_code == 200
            self.committed.set()
            await self.release.wait()

        return response


@pytest.mark.parametrize("operation", ["claim", "write"])
async def test_cancelled_task_write_retains_revision(
    app: FastAPI, dispatch: DispatchFixture, postgres_session: Session, operation: str
) -> None:
    """Settle a committed API write before cancellation releases the attempt lock.

    Test cases:
    - Cancellation during claim retains the claimed revision.
    - Cancellation after a write commits does not leave the next write using an old revision.
    - Repeated cancellation cannot let another writer pass an unfinished mutation.
    """
    task = postgres_session.get(Task, dispatch.task_id)
    assert task is not None
    task.status = TaskStatus.PENDING
    postgres_session.add(task)
    postgres_session.commit()
    transport = MockPausedResponseTransport(app, f"tasks/{task.id}/{operation}")
    async with httpx.AsyncClient(transport=transport, base_url="http://tracker.test") as http:
        api = ExecutorClient(
            ExecutorTransport(http, SecretStr(dispatch.token)),
            dispatch.dispatch_id,
            UUID(dispatch.claim["claimant_id"]),
        )
        await api.claim(ClaimRequest.model_validate(dispatch.claim))
        state = await api.run_state([task.task_id])
        persistence = ApiTaskPersistence(api, state.tasks[0])
        if operation == "claim":
            pending = asyncio.create_task(persistence.load())
        else:
            assert await persistence.load() is not None
            pending = asyncio.create_task(persistence.write(BuildTask()))

        async with asyncio.timeout(5):
            await transport.committed.wait()
            pending.cancel()
            await asyncio.sleep(0)
            pending.cancel()
            next_write = asyncio.create_task(persistence.write(BuildTask() if operation == "claim" else RunTask()))
            await asyncio.sleep(0)
            assert not pending.done() and not next_write.done()
            transport.release.set()
            with pytest.raises(asyncio.CancelledError):
                await pending
            assert await next_write

    postgres_session.refresh(task)
    assert task.status == (TaskStatus.BUILDING if operation == "claim" else TaskStatus.IN_PROGRESS)
    owner = postgres_session.get(ExecutorTaskAttempt, task.id)
    assert owner is not None and owner.revision == (1 if operation == "claim" else 2)


@pytest.mark.parametrize("operation", ["claim", "heartbeat", "finish", "fail", "run/initialize", "run/state"])
async def test_client_recovers_a_response_lost_after_commit(
    app: FastAPI, dispatch: DispatchFixture, assigned_run: list[str], postgres_session: Session, operation: str
) -> None:
    """Retry committed operations through the real client and PostgreSQL-backed API.

    Test cases:
    - Claim and heartbeat survive a lost response without changing the claimant.
    - Terminal retry returns its receipt and never duplicates failure records.
    - Task initialization and state reads survive lost responses without resetting attempts.
    """
    lost_response = MockLostResponseTransport(app, operation)
    async with httpx.AsyncClient(transport=lost_response, base_url="http://tracker.test") as http_client:
        client = ExecutorClient(
            ExecutorTransport(http_client, SecretStr(dispatch.token)),
            dispatch.dispatch_id,
            UUID(dispatch.claim["claimant_id"]),
        )
        claimed = await client.claim(ClaimRequest.model_validate(dispatch.claim))
        assert claimed.dispatch_id == dispatch.dispatch_id
        assert (await client.authority()).current
        initialized = await client.initialize_run_tasks(assigned_run)
        polled = await client.run_state(assigned_run)
        assert [task.id for task in polled.tasks] == [task.id for task in initialized.tasks]
        assert [task.started_at for task in polled.tasks] == [task.started_at for task in initialized.tasks]
        assert all(task.eval_resume_state is None for task in polled.tasks)
        resumed = await client.run_state(assigned_run, include_eval_resume_state=True)
        assert resumed.tasks[1].eval_resume_state == initialized.tasks[1].eval_resume_state
        assert resumed.run.started_by_email == "executor-test@example.com"
        await client.heartbeat()
        terminal = await client.fail("test failure") if operation == "fail" else await client.finish()
        assert terminal.status == ("FAILED" if operation == "fail" else "FINISHED")
        assert not (await client.authority()).current

    assert lost_response.dropped
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    assert invocation is not None
    assert invocation.status.value == terminal.status
    errors = postgres_session.exec(select(ErrorResult).where(ErrorResult.task == dispatch.task_id)).all()
    assert len(errors) == (1 if operation == "fail" else 0)


@pytest.mark.parametrize("lost_operation", ["claim", "write"])
async def test_task_client_retries_after_committed_response_loss(
    app: FastAPI, dispatch: DispatchFixture, task_attempt: TaskFixture, postgres_session: Session, lost_operation: str
) -> None:
    """Retry task ownership and writes through the real client after server commit.

    Test cases:
    - Retried task claims keep revision zero and the same owner.
    - A lost write response does not increment the revision twice or duplicate results.
    """
    transport = MockLostResponseTransport(
        app, f"tasks/{task_attempt.id}/{lost_operation}", mutation="complete" if lost_operation == "write" else None
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://tracker.test") as http_client:
        api = ExecutorClient(
            ExecutorTransport(http_client, SecretStr(dispatch.token)),
            dispatch.dispatch_id,
            UUID(dispatch.claim["claimant_id"]),
        )
        started_at = datetime.fromisoformat(task_attempt.request["expected_started_at"])
        claimed = await api.claim_task(task_attempt.id, started_at, command_id=uuid4())
        assert claimed.revision == 0
        for revision, mutation in enumerate(
            [BuildTask(), RunTask(), EvaluateTask(), CompleteTask(result={"score": 1})]
        ):
            receipt = await api.write_task(
                task_attempt.id, started_at, mutation, command_id=uuid4(), expected_revision=revision
            )
            assert receipt.revision == revision + 1
    assert transport.dropped
    results = postgres_session.exec(select(EvaluationResult).where(EvaluationResult.task == task_attempt.id)).all()
    assert len(results) == 1
    owner = postgres_session.get(ExecutorTaskAttempt, task_attempt.id)
    assert owner is not None and owner.revision == 4


@pytest.mark.parametrize("retry_sandbox", [False, True])
async def test_process_task_persists_through_api(
    app: FastAPI,
    dispatch: DispatchFixture,
    postgres_session: Session,
    monkeypatch: pytest.MonkeyPatch,
    retry_sandbox: bool,
) -> None:
    """Run the task lifecycle through PostgreSQL-backed HTTP without executor SQL.

    Test cases:
    - Execution persists checkpoints, durations, and the final result using v1 commands.
    - Losing the completion response does not duplicate the result.
    - A fresh sandbox retry retains the task attempt and its write revision.
    """
    task = postgres_session.get(Task, dispatch.task_id)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert task is not None and benchmark is not None
    org = postgres_session.get(Org, benchmark.org_id)
    assert org is not None
    task.status = TaskStatus.PENDING
    postgres_session.add(task)
    postgres_session.commit()

    def forbidden_session(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("API-backed task execution opened a database session")

    @asynccontextmanager
    async def sandbox(*_args: Any, **_kwargs: Any) -> AsyncGenerator[Sandbox]:
        instance = Mock(spec=Sandbox, id="api-sandbox")
        instance.name = "api-sandbox"
        yield instance

    agent_calls = 0

    async def agent(
        *_args: Any, execution_is_current: Callable[[], Awaitable[bool]], **_kwargs: Any
    ) -> tuple[None, float]:
        nonlocal agent_calls
        agent_calls += 1
        assert await execution_is_current()
        if retry_sandbox and agent_calls == 1:
            raise SandboxSetupError("Provider rejected the first command stream")
        return None, 2.5

    async def evaluate(*_args: Any, on_eval_resume_state: CheckpointCallback, **_kwargs: Any) -> dict[str, Any]:
        on_eval_resume_state({"cursor": 1})
        on_eval_resume_state({"cursor": 2})
        return {"score": 1}

    monkeypatch.setattr("tracker.utils.task_execution.Session", forbidden_session)
    monkeypatch.setattr("tracker.utils.task_execution.create_sandbox", sandbox)
    monkeypatch.setattr("tracker.utils.task_execution.upload_agent_artifacts", AsyncMock())
    monkeypatch.setattr("tracker.utils.task_execution.run_agent", agent)
    monkeypatch.setattr(BenchmarkServiceClient, "evaluate_instance", evaluate)
    monkeypatch.setattr(BenchmarkServiceClient, "setup_task", AsyncMock())
    monkeypatch.setattr(
        BenchmarkServiceClient,
        "retrieve_task",
        AsyncMock(
            return_value=RetrieveTaskResponse(
                source=ImageSource(image="test-image:latest"),
                problem_path="/tmp/problem.txt",
                cwd="/testbed",
                resources=Resources(vcpu=2, memory=4, disk=5),
            )
        ),
    )
    runtime = Mock(spec=RuntimeServices, logs=Mock(), objects=Mock(), log_locations=Mock())
    runtime.resolve_secrets = AsyncMock(return_value={})
    transport = MockLostResponseTransport(app, "write", mutation="complete")
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://tracker.test") as http,
        BenchmarkServiceClient("http://benchmark.test", {}) as service,
    ):
        api = ExecutorClient(
            ExecutorTransport(http, SecretStr(dispatch.token)),
            dispatch.dispatch_id,
            UUID(dispatch.claim["claimant_id"]),
        )
        await api.claim(ClaimRequest.model_validate(dispatch.claim))
        state = await api.run_state([task.task_id])
        result = await process_task(
            task,
            StartBenchmarkRequest(
                benchmark_name=benchmark.name,
                contract=benchmark.arguments.contract,
                task_ids=[task.task_id],
                concurrency=1,
            ),
            service,
            benchmark.id,
            task.task_id,
            runtime,
            org,
            DaytonaProviderConfig(
                DAYTONA_API_KEY="test", DAYTONA_API_URL="https://app.daytona.io/api", DAYTONA_TARGET="us"
            ),
            asyncio.Semaphore(1),
            ExecutionAuthority(benchmark.id, dispatch.dispatch_id),
            sandbox_provider=Mock(spec=SandboxProvider),
            persistence=ApiTaskPersistence(api, state.tasks[0]),
        )

    assert result == {task.task_id: {"score": 1}}
    assert transport.dropped
    postgres_session.refresh(task)
    assert task.status == TaskStatus.FINISHED
    results = postgres_session.exec(select(EvaluationResult).where(EvaluationResult.task == task.id)).all()
    assert len(results) == 1 and results[0].result == {"score": 1}
    assert task.task_breakdown is not None
    breakdown = postgres_session.get(TaskBreakdown, task.task_breakdown)
    assert breakdown is not None and breakdown.agent_run_duration == 2.5
    owner = postgres_session.get(ExecutorTaskAttempt, task.id)
    assert owner is not None and owner.revision == (10 if retry_sandbox else 6)
    errors = postgres_session.exec(select(ErrorResult).where(ErrorResult.task == task.id)).all()
    assert len(errors) == (1 if retry_sandbox else 0)
    if errors:
        assert errors[0].retry_scheduled and errors[0].failed_attempt_number == 1


@pytest.mark.parametrize("revocation", ["dispatch", "attempt", "stop", "revision"])
async def test_task_persistence_rejects_stale_writes(
    app: FastAPI, dispatch: DispatchFixture, postgres_session: Session, revocation: str
) -> None:
    """Keep resumed evaluation checkpoints while fencing obsolete task writers.

    Test cases:
    - A claimed evaluation resumes from its durable checkpoint.
    - Revoked dispatches, stopped tasks, and replacement attempts reject further writes.
    - A concurrent revision conflict stays rejected on subsequent calls.
    """
    task = postgres_session.get(Task, dispatch.task_id)
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    assert task is not None and invocation is not None and benchmark is not None
    task.status = TaskStatus.EVALUATING
    task.started_at = invocation.created_at
    task.eval_resume_state = {"cursor": 3}
    benchmark.started_by_email = "executor-test@example.com"
    postgres_session.add_all([task, benchmark])
    postgres_session.commit()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://tracker.test") as http:
        api = ExecutorClient(
            ExecutorTransport(http, SecretStr(dispatch.token)),
            dispatch.dispatch_id,
            UUID(dispatch.claim["claimant_id"]),
        )
        await api.claim(ClaimRequest.model_validate(dispatch.claim))
        state = await api.run_state([task.task_id])
        persistence = ApiTaskPersistence(api, state.tasks[0])
        snapshot = await persistence.load()
        assert snapshot is not None and snapshot.identity["email"] == "executor-test@example.com"
        assert await persistence.resume() == {"cursor": 3}

        if revocation == "dispatch":
            invocation.status = ExecutorDispatchStatus.FAILED
            postgres_session.add(invocation)
        elif revocation == "attempt":
            task.started_at += timedelta(seconds=1)
            postgres_session.add(task)
        elif revocation == "stop":
            task.status = TaskStatus.STOPPED
            postgres_session.add(task)
        else:
            owner = postgres_session.get(ExecutorTaskAttempt, task.id)
            assert owner is not None
            owner.revision += 1
            postgres_session.add(owner)
        postgres_session.commit()

        if revocation != "revision":
            assert not await persistence.current()
        assert not await persistence.write(CompleteTask(result={"score": 1}))
        assert not await persistence.write(CompleteTask(result={"score": 1}))
        assert await persistence.load() is None
        assert await persistence.resume() is None

    postgres_session.refresh(task)
    assert task.eval_resume_state == {"cursor": 3}
    assert task.status == (TaskStatus.STOPPED if revocation == "stop" else TaskStatus.EVALUATING)
    assert not postgres_session.exec(select(EvaluationResult).where(EvaluationResult.task == task.id)).all()


@pytest.mark.parametrize("operation", ["run/finalization", "run/finalize"])
async def test_finalization_client_recovers_lost_response(
    app: FastAPI, dispatch: DispatchFixture, finalizable_run: str, postgres_session: Session, operation: str
) -> None:
    """Retry finalization through the real client after a committed response is lost.

    Test cases:
    - Snapshot reads retain the same digest across transport retries.
    - A lost completion response does not duplicate the final score.
    """
    transport = MockLostResponseTransport(app, operation)
    async with httpx.AsyncClient(transport=transport, base_url="http://tracker.test") as http_client:
        api = ExecutorClient(
            ExecutorTransport(http_client, SecretStr(dispatch.token)),
            dispatch.dispatch_id,
            UUID(dispatch.claim["claimant_id"]),
        )
        state = await api.finalization_state()
        assert state.snapshot_digest == finalizable_run
        receipt = await api.finalize_run(finalizable_run, CompleteRun(final_score=0.75), command_id=uuid4())
        assert receipt.status == "FINISHED" and receipt.benchmark_id == dispatch.benchmark_id
        assert (await api.finish()).status == "FINISHED"

    assert transport.dropped
    assert len(postgres_session.exec(select(FinalEvaluation)).all()) == 1
    assert len(postgres_session.exec(select(ExecutorRunReceipt)).all()) == 1


@pytest.mark.parametrize("operation", ["reserve", "release"])
async def test_queue_client_recovers_lost_response(
    app: FastAPI, dispatch: DispatchFixture, queued_task: TaskFixture, postgres_session: Session, operation: str
) -> None:
    """Retry pool reservations and settled releases after the committed response is lost.

    Test cases:
    - Retrying reservation does not increment the task revision twice.
    - Retrying release does not restore the reservation or change the running task.
    """
    transport = MockLostResponseTransport(app, f"queue/{operation}")
    async with httpx.AsyncClient(transport=transport, base_url="http://tracker.test") as http_client:
        api = ExecutorClient(
            ExecutorTransport(http_client, SecretStr(dispatch.token)),
            dispatch.dispatch_id,
            UUID(dispatch.claim["claimant_id"]),
        )
        started_at = datetime.fromisoformat(queued_task.request["expected_started_at"])
        reservation = await api.reserve_pool(queued_task.id, started_at, command_id=uuid4(), expected_revision=0)
        assert reservation.reserved and reservation.revision == 1 and reservation.reservation_id is not None
        await api.write_task(queued_task.id, started_at, BuildTask(), command_id=uuid4(), expected_revision=1)
        await api.write_task(queued_task.id, started_at, RunTask(), command_id=uuid4(), expected_revision=2)
        released = await api.release_pool(queued_task.id, started_at, reservation.reservation_id, command_id=uuid4())
        assert released.released

    assert transport.dropped
    assert postgres_session.exec(select(ExecutorPoolReservation)).first() is None
    owner = postgres_session.get(ExecutorTaskAttempt, queued_task.id)
    task = postgres_session.get(Task, queued_task.id)
    assert owner is not None and owner.revision == 3
    assert task is not None and task.status == TaskStatus.IN_PROGRESS


async def _stop_api_process(process: asyncio.subprocess.Process) -> None:
    if process.stdin is not None:
        process.stdin.close()
    if process.returncode is None:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=10)
        except TimeoutError:
            process.kill()
            await process.wait()


async def _process_event(process: asyncio.subprocess.Process) -> dict[str, object]:
    assert process.stdout is not None
    async with asyncio.timeout(15):
        line = await process.stdout.readline()
        if not line:
            assert process.stderr is not None
            pytest.fail(f"API test process exited: {(await process.stderr.read()).decode()}")

    return json.loads(line)


async def _start_api_server(
    stack: AsyncExitStack, database_url: str, port: int = 0, *, checkpoint_failures: int = 0
) -> tuple[asyncio.subprocess.Process, int]:
    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
        port = listener.getsockname()[1]
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(Path(__file__).with_name("api_server.py")),
            str(listener.fileno()),
            pass_fds=(listener.fileno(),),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={
                **os.environ,
                "TEST_EXECUTOR_DATABASE_URL": database_url,
                "TEST_EXECUTOR_CHECKPOINT_FAILURES": str(checkpoint_failures),
            },
        )
    stack.push_async_callback(_stop_api_process, process)
    assert (await _process_event(process))["event"] == "ready"

    return process, port


async def test_api_server_restart_preserves_executor_process(
    dispatch: DispatchFixture, postgres_engine: Engine, postgres_session: Session
) -> None:
    """Restart an actual API process while a database-free client process retains its claim.

    Test cases:
    - A new server process accepts the original client's claim and task attempt.
    - The same client PID completes the task and run through real HTTP after restart.
    - No executor-side database/config modules or connections are required.
    - Checkpoint writes survive seven failed requests while heartbeat authority remains valid.
    """
    task = postgres_session.get(Task, dispatch.task_id)
    assert task is not None
    task.status = TaskStatus.PENDING
    postgres_session.add(task)
    postgres_session.commit()
    database_url = postgres_engine.url.render_as_string(hide_password=False)
    async with AsyncExitStack() as stack:
        server, port = await _start_api_server(stack, database_url)
        worker = await asyncio.create_subprocess_exec(
            sys.executable,
            str(Path(__file__).with_name("api_worker.py")),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stack.push_async_callback(_stop_api_process, worker)
        assert worker.stdin is not None
        worker.stdin.write(
            json.dumps(
                {
                    "endpoint": f"http://127.0.0.1:{port}",
                    "token": dispatch.token,
                    "dispatch_id": str(dispatch.dispatch_id),
                    "claim": dispatch.claim,
                    "task_ids": ["task-0"],
                }
            ).encode()
            + b"\n"
        )
        await worker.stdin.drain()
        started = await _process_event(worker)
        assert started["event"] == "started"
        postgres_session.refresh(task)
        assert task.status == TaskStatus.IN_PROGRESS

        await _stop_api_process(server)
        assert server.returncode is not None and worker.returncode is None
        worker.stdin.write(b"read\n")
        await worker.stdin.drain()
        assert (await _process_event(worker))["event"] == "reading"
        replacement, _ = await _start_api_server(stack, database_url, port, checkpoint_failures=7)
        assert replacement.pid != server.pid
        resumed = await _process_event(worker)
        finished = await _process_event(worker)
        assert resumed == {"event": "resumed", "pid": started["pid"]}
        assert finished == {"event": "finished", "pid": started["pid"]}
        assert await asyncio.wait_for(worker.wait(), timeout=10) == 0

    postgres_session.expire_all()
    task = postgres_session.get(Task, dispatch.task_id)
    benchmark = postgres_session.get(Benchmark, dispatch.benchmark_id)
    invocation = postgres_session.get(ExecutorDispatch, dispatch.dispatch_id)
    assert task is not None and task.status == TaskStatus.FINISHED
    owner = postgres_session.get(ExecutorTaskAttempt, task.id)
    assert owner is not None and owner.revision == 6
    assert task.started_at == datetime.fromisoformat(str(started["started_at"])).replace(tzinfo=None)
    assert benchmark is not None and benchmark.status == BenchmarkStatus.FINISHED
    assert invocation is not None and invocation.status == ExecutorDispatchStatus.FINISHED
    assert benchmark.final_evaluation is not None and benchmark.final_evaluation.final_score == 1
