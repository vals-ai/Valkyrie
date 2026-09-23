"""Exercise executor v1 contracts and claim races against disposable PostgreSQL.

Run: uv run pytest tests/integration/local/database/test_dispatch_api.py
Requests use literal v1 payloads so client and server schema changes cannot mask regressions.
"""

from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Barrier
from uuid import UUID, uuid4

import pytest
import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy.engine import Engine
from pydantic import SecretStr
from sqlmodel import Session, select

from tests.factories import make_benchmark, make_task
from tracker.aws.runtime import AWSResources
from tracker.database.models import (
    Benchmark,
    BenchmarkStatus,
    ErrorResult,
    ExecutorDispatch,
    ExecutorDispatchAccess,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    Org,
    Task,
    TaskStatus,
)
from tracker.database.session import get_session
from tracker.executor.dispatch_api import create_dispatch_access
from tracker.executor.release_control import create_executor_dispatch, pin_benchmark_to_release, register_release
from tracker.executor_api.v1.router import router
from tracker.executor_api.transport import ExecutorTransport
from tracker.executor_api.v1.client import ExecutorClient
from tracker.executor_api.v1.schemas import ClaimRequest


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
    "operation", ["claim", "authority", "heartbeat", "finish", "fail", "run/initialize", "run/state"]
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
    assert replay.json() == first.json()
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


class MockLostResponseTransport(httpx.AsyncBaseTransport):
    """Drop a successful response only after the real API has committed it."""

    def __init__(self, app: FastAPI, operation: str) -> None:
        self._transport = httpx.ASGITransport(app)
        self._operation = operation
        self.dropped = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self._transport.handle_async_request(request)
        if request.url.path.endswith(f"/{self._operation}") and response.status_code == 200 and not self.dropped:
            self.dropped = True
            await response.aclose()
            raise httpx.ReadError("Connection lost after server commit", request=request)

        return response

    async def aclose(self) -> None:
        await self._transport.aclose()


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
