"""Tests for Tracker FastAPI route behavior.

Run: uv run pytest tests/unit/test_main.py
"""

import io
import logging
import tarfile
from collections.abc import AsyncIterator
from datetime import timezone
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import httpx
import pytest
from benchmark_service.client import (
    BenchmarkServiceClient,
    BenchmarkServiceError,
    BenchmarkServiceUnauthenticatedError,
)
from benchmark_service.schemas import FinalScoreResponse, VerifyTaskIdsResponse
from dateutil.parser import isoparse
from descope.descope_client import DescopeClient
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pytest import MonkeyPatch
from sqlmodel import Session, select
from taskiq.message import BrokerMessage, TaskiqMessage

import main as main_module
import services.executor_host.supervisor as executor_host  # pyright: ignore[reportMissingImports]
from executor_protocol import SUPPORTED_PROTOCOL_VERSION, ExecutorTelemetryContext
from main import app, tracker_service_error_handler
from tests.utils import TEST_ORG_ID, async_iterator
from tracker.auth import RequestIdentity, get_current_org, get_current_starter
from tracker.aws.runtime import AWSRuntime
from tracker.runtime.storage import StoredObject, StoredObjectCopy
from tracker.database.models import (
    AgentContractRequest,
    Benchmark,
    BenchmarkArguments,
    BenchmarkStatus,
    ExecutorAdmission,
    ExecutorDispatch,
    ExecutorDispatchKind,
    ExecutorDispatchStatus,
    ExecutorRelease,
    ExecutorReleaseStatus,
    DocentReadingStatus,
    ErrorResult,
    EvaluationResult,
    FinalEvaluation,
    Org,
    Task,
    TaskStatus,
)
from tracker.config import STABLE_QUEUE_NAME
from tracker.exceptions import TrackerServiceError
from tracker.types import (
    BenchmarkTableRow,
    FetchBenchmarksRequest,
    FinalViewResponse,
    HarnessConfig,
    StartBenchmarkRequest,
)
from tracker.utils import update_benchmark_concurrency

client = TestClient(app)


@pytest.fixture(autouse=True)
def active_executor_release(database_session: Session) -> None:
    """Provide the admission target required by benchmark-start tests."""
    release = ExecutorRelease(
        id="test-release",
        artifact_uri="s3://artifacts/test-release.pex",
        artifact_digest="digest-test-release",
        protocol_version=SUPPORTED_PROTOCOL_VERSION,
        status=ExecutorReleaseStatus.ACTIVE,
        readiness_verified=True,
    )
    admission = database_session.get(ExecutorAdmission, 1)
    assert admission is not None
    admission.release_id = release.id
    database_session.add(release)
    database_session.add(admission)
    database_session.commit()


async def _verify_single_task_id(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
    """Return the single task used by benchmark start route tests."""
    return VerifyTaskIdsResponse(task_ids=["task_0"])


class TestTrackerAPI:
    """Tracker API route behavior and error responses."""

    async def _mock_verify_task_ids_error(self, *_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
        raise Exception("Error verifying task ids")

    def test_health_check(self, monkeypatch: MonkeyPatch) -> None:
        """Test health check of the fastapi server.

        Test Cases:
            - Returns 200 OK
            - Response contains expected format
        """
        # Mock database connection check for unit tests
        monkeypatch.setattr("main.check_database_connection", lambda: True)

        response = client.get("/health")

        assert response.status_code == 200

        assert response.json() == {"status": "ok"}

    def test_executor_release_status_is_not_exposed_over_http(self) -> None:
        response = client.get("/executor-releases")

        assert response.status_code == 404

    @pytest.mark.parametrize("concurrency", [0, -1, 1.5, "2", True])
    def test_update_benchmark_concurrency_rejects_non_positive_or_non_integer_values(
        self,
        concurrency: object,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        response = client.patch(
            f"/benchmarks/{example_benchmark_object.id}/concurrency",
            json={"concurrency": concurrency},
        )

        assert response.status_code == 422
        database_session.refresh(example_benchmark_object)
        assert example_benchmark_object.arguments.concurrency == 5

    def test_update_benchmark_concurrency_persists_full_arguments_and_is_idempotent(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        original_arguments = example_benchmark_object.arguments
        database_session.add(example_benchmark_object)
        database_session.commit()

        first_response = client.patch(
            f"/benchmarks/{example_benchmark_object.id}/concurrency",
            json={"concurrency": 9},
        )
        retry_response = client.patch(
            f"/benchmarks/{example_benchmark_object.id}/concurrency",
            json={"concurrency": 9},
        )

        expected_response = {
            "benchmark_id": str(example_benchmark_object.id),
            "status": BenchmarkStatus.IN_PROGRESS.value,
            "concurrency": 9,
        }
        assert first_response.status_code == 200
        assert first_response.json() == expected_response
        assert retry_response.status_code == 200
        assert retry_response.json() == expected_response

        database_session.expire_all()
        persisted = database_session.get(Benchmark, example_benchmark_object.id)
        assert persisted is not None
        assert persisted.arguments == original_arguments.model_copy(update={"concurrency": 9})

    def test_update_benchmark_concurrency_hides_missing_and_other_org_runs(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()
        missing_response = client.patch(
            f"/benchmarks/{uuid4()}/concurrency",
            json={"concurrency": 9},
        )
        other_org = Org(id=uuid4(), name="other")
        monkeypatch.setitem(app.dependency_overrides, get_current_org, lambda: other_org)

        response = client.patch(
            f"/benchmarks/{example_benchmark_object.id}/concurrency",
            json={"concurrency": 9},
        )

        assert missing_response.status_code == 404
        assert missing_response.json() == {"detail": "Not found"}
        assert response.status_code == 404
        assert response.json() == {"detail": "Not found"}
        database_session.refresh(example_benchmark_object)
        assert example_benchmark_object.arguments.concurrency == 5

    def test_update_benchmark_concurrency_rejects_non_active_run(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        example_benchmark_object.status = BenchmarkStatus.STOPPED
        database_session.add(example_benchmark_object)
        database_session.commit()

        response = client.patch(
            f"/benchmarks/{example_benchmark_object.id}/concurrency",
            json={"concurrency": 9},
        )

        assert response.status_code == 409
        database_session.refresh(example_benchmark_object)
        assert example_benchmark_object.arguments.concurrency == 5

    def test_update_benchmark_concurrency_refreshes_preloaded_state_before_mutating(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        with Session(bind=database_session.get_bind()) as concurrent_session:
            concurrent_benchmark = concurrent_session.get(Benchmark, example_benchmark_object.id)
            assert concurrent_benchmark is not None
            concurrent_benchmark.status = BenchmarkStatus.STOPPED
            concurrent_session.add(concurrent_benchmark)
            concurrent_session.commit()

        result = update_benchmark_concurrency(
            example_benchmark_object.id,
            9,
            database_session,
            Org(id=TEST_ORG_ID, name="default"),
        )

        assert result.status == BenchmarkStatus.STOPPED
        database_session.expire_all()
        persisted = database_session.get(Benchmark, example_benchmark_object.id)
        assert persisted is not None
        assert persisted.status == BenchmarkStatus.STOPPED
        assert persisted.arguments.concurrency == 5

    def test_update_benchmark_concurrency_returns_snapshot_from_locked_transaction(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()
        commit_update = database_session.commit

        def commit_then_stop() -> None:
            commit_update()
            with Session(bind=database_session.get_bind()) as concurrent_session:
                concurrent_benchmark = concurrent_session.get(Benchmark, example_benchmark_object.id)
                assert concurrent_benchmark is not None
                concurrent_benchmark.status = BenchmarkStatus.STOPPED
                concurrent_session.add(concurrent_benchmark)
                concurrent_session.commit()

        monkeypatch.setattr(database_session, "commit", commit_then_stop)

        result = update_benchmark_concurrency(
            example_benchmark_object.id,
            9,
            database_session,
            Org(id=TEST_ORG_ID, name="default"),
        )

        assert result.status == BenchmarkStatus.IN_PROGRESS
        database_session.expire_all()
        persisted = database_session.get(Benchmark, example_benchmark_object.id)
        assert persisted is not None
        assert persisted.status == BenchmarkStatus.STOPPED
        assert persisted.arguments.concurrency == 9

    def test_trailing_slash_does_not_redirect(self, monkeypatch: MonkeyPatch) -> None:
        """
        Verify non-canonical route paths cannot construct redirects from forwarded request data.

        Test cases:
        - The canonical health path remains available.
        - The trailing-slash variant returns not found without a Location header.
        """
        monkeypatch.setattr("main.check_database_connection", lambda: True)

        canonical_response = client.get("/health")
        slash_response = client.get(
            "/health/",
            headers={"Host": "example.invalid", "X-Forwarded-Proto": "https"},
            follow_redirects=False,
        )

        assert canonical_response.status_code == 200
        assert slash_response.status_code == 404
        assert "location" not in slash_response.headers

    def test_analyze_benchmark_enforces_state_and_reuses_cached_result(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        """Docent analysis must reject invalid runs and return a completed cached result.

        Test cases:
        - An active run cannot be analyzed.
        - A finished uncached run requires an analyzer Lambda.
        - A finished cached run returns its stored reading-plan URL without invoking Lambda.
        """
        database_session.add(example_benchmark_object)
        database_session.commit()

        active_response = client.post(
            f"/analyze-benchmark/{example_benchmark_object.id}",
            json={"lambda_function": "docent-analyzer"},
            headers=harness_headers,
        )
        assert active_response.status_code == 400
        assert "must be FINISHED" in active_response.json()["detail"]

        example_benchmark_object.status = BenchmarkStatus.FINISHED
        database_session.add(example_benchmark_object)
        database_session.commit()
        missing_lambda_response = client.post(
            f"/analyze-benchmark/{example_benchmark_object.id}",
            json={},
            headers=harness_headers,
        )
        assert missing_lambda_response.status_code == 400
        assert "No ingest_lambda provided" in missing_lambda_response.json()["detail"]

        example_benchmark_object.docent_reading_status = DocentReadingStatus.DONE
        example_benchmark_object.docent_reading_url = "https://results.example/reading-plan"
        database_session.add(example_benchmark_object)
        database_session.commit()
        cached_response = client.post(
            f"/analyze-benchmark/{example_benchmark_object.id}",
            json={},
            headers=harness_headers,
        )
        assert cached_response.status_code == 200
        assert cached_response.json() == {
            "status": "done",
            "reading_plan_url": "https://results.example/reading-plan",
        }

    async def test_tracker_service_error_hides_internal_detail(
        self,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """
        Verify internal Tracker errors retain diagnostics only in server logs.

        Test cases:
        - The client receives a stable generic detail.
        - The full exception remains available to operators in logs.
        """
        sensitive_detail = "sensitive-internal-tracker-detail"

        def capture_exception(_exception: BaseException) -> None:
            return None

        monkeypatch.setattr("sentry_sdk.capture_exception", capture_exception)

        with pytest.raises(HTTPException) as exc_info:
            await tracker_service_error_handler(MagicMock(), TrackerServiceError(sensitive_detail))

        assert exc_info.value.status_code == 500
        assert exc_info.value.detail == "Tracker service operation failed"
        assert sensitive_detail in caplog.text

    async def test_fetch_benchmark_tasks_hides_downstream_error(self, monkeypatch: MonkeyPatch) -> None:
        """
        Verify downstream benchmark-service diagnostics are not reflected to callers.

        Test cases:
        - The endpoint retains its gateway-error status.
        - The downstream sentinel is absent from the stable response detail.
        """
        sensitive_detail = "sensitive-downstream-provider-detail"

        async def _raise_downstream_error(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            raise BenchmarkServiceError(sensitive_detail)

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _raise_downstream_error)

        response = client.post(
            "/fetch-benchmark-tasks",
            json={"benchmark_name": "swebench", "dataset": "verified"},
        )

        assert response.status_code == 502
        assert response.json() == {"detail": "Failed to fetch task ids from benchmark service"}
        assert sensitive_detail not in response.text

    async def test_fetch_benchmark_tasks(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        async def _mock_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=["task_1", "task_2"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)

        response = client.post(
            "/fetch-benchmark-tasks",
            json={
                "benchmark_name": "swebench",
                "dataset": "verified",
            },
        )

        assert response.status_code == 200
        assert response.json() == {"task_ids": ["task_1", "task_2"]}

    async def test_fetch_benchmark_tasks_forwards_tracker_key_only_to_hosted_origin(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        observed_headers: list[dict[str, str]] = []

        async def _mock_verify_task_ids(
            service_client: BenchmarkServiceClient,
            *_args: Any,
            **_kwargs: Any,
        ) -> VerifyTaskIdsResponse:
            observed_headers.append(dict(getattr(service_client, "_headers")))
            return VerifyTaskIdsResponse(task_ids=["task_1"])

        monkeypatch.setattr("tracker.config._BENCHMARK_SERVICE_BASE_URL", "benchmarks.vals.ai")
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)

        for service_url in (
            "https://swebench.benchmarks.vals.ai:443/path",
            "https://team.example",
        ):
            response = client.post(
                "/fetch-benchmark-tasks",
                json={
                    "benchmark_name": "swebench",
                    "custom_benchmark_service": service_url,
                },
                headers={"X-Api-Key": "tracker-api-key"},
            )
            assert response.status_code == 200

        assert observed_headers[0]["X-Descope-Api-Key"] == "tracker-api-key"
        assert "X-Descope-Api-Key" not in observed_headers[1]

    async def test_fetch_benchmark_tasks_blocks_external_internal_custom_destination(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(main_module, "AUTH_REQUIRED", True)

        blocked = client.post(
            "/fetch-benchmark-tasks",
            json={
                "benchmark_name": "swebench",
                "custom_benchmark_service": "https://benchmarks-dev.vals.ai",
            },
        )
        canonical = client.post(
            "/fetch-benchmark-tasks",
            json={"benchmark_name": "swebench"},
        )

        assert blocked.status_code == 403
        assert blocked.json() == {"detail": "Custom benchmark destination is not allowed"}
        assert canonical.status_code == 200

    async def test_start_benchmark(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
        mock_kicker: Any,
    ) -> None:
        """Test start benchmark of the fastapi server.

        Test Cases:
            - Returns 200 OK
            - Start timestamp is in UTC timezone
            - Returning task count provided from the verify_task_ids function
            - Benchmark row has been created and pushed to the database
        """

        # Example request sent from the cli to the fastapi server
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=harness_config,
        )

        async def _mock_verify_task_ids(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            return VerifyTaskIdsResponse(task_ids=[f"task_{i}" for i in range(500)])

        monkeypatch.setattr(
            BenchmarkServiceClient,
            "verify_task_ids",
            _mock_verify_task_ids,
        )

        # Send request to start the run and ensure that the start response is returned
        response = client.post(
            "/start-benchmark",
            json=request.model_dump(),
        )

        # Test case 1. Returns 200 OK
        assert response.status_code == 200
        json_response = response.json()

        # Test case 2. Benchmark row has been created and pushed to the database
        benchmark_row = database_session.get(Benchmark, UUID(json_response["benchmark_id"]))
        assert benchmark_row

        # Secondary test. Arguments is correct serialized into the database
        assert benchmark_row.arguments == BenchmarkArguments(
            contract=request.contract,
            concurrency=request.concurrency,
            task_ids=None,
            slice_str=None,
            sandbox_provider_secret_name=harness_config.sandbox_provider_secret_name,
        )

        # Test case 3. Start timestamp is in UTC timezone and matches the benchmark row
        assert isoparse(json_response["started_at"]) == benchmark_row.started_at.replace(tzinfo=timezone.utc)

        # Test case 4. Returning task count provided from the verify_task_ids function
        assert json_response["task_count"] == 500

        assert benchmark_row.executor_release_id == "test-release"
        assert benchmark_row.current_execution_release_id == "test-release"
        assert benchmark_row.executor_artifact_uri == "s3://artifacts/test-release.pex"
        assert benchmark_row.executor_artifact_digest == "digest-test-release"
        assert benchmark_row.executor_protocol_version == SUPPORTED_PROTOCOL_VERSION
        queued_call = mock_kicker.queued_calls[0]
        dispatch_id = UUID(queued_call["executor_dispatch_id"])
        dispatch = database_session.get(ExecutorDispatch, dispatch_id)
        assert dispatch is not None
        assert dispatch.benchmark_id == benchmark_row.id
        assert dispatch.kind == ExecutorDispatchKind.START
        assert dispatch.status == ExecutorDispatchStatus.QUEUED
        assert dispatch.executor_release_id == "test-release"
        assert dispatch.executor_artifact_uri == "s3://artifacts/test-release.pex"
        assert dispatch.executor_artifact_digest == "digest-test-release"
        assert dispatch.executor_protocol_version == SUPPORTED_PROTOCOL_VERSION
        assert queued_call["verified_task_ids"] == [f"task_{i}" for i in range(500)]
        task_rows = database_session.exec(select(Task).where(Task.benchmark == benchmark_row.id)).all()
        assert len(task_rows) == 500
        assert all(task.started_at <= dispatch.created_at for task in task_rows)

        # Remaining fields match what we passed into the request
        assert json_response["benchmark_name"] == request.benchmark_name
        assert json_response["agent_name"] == request.contract.name
        assert json_response["executor_release_id"] == "test-release"
        assert json_response["current_execution_release_id"] == "test-release"
        assert json_response["executor_artifact_digest"] == "digest-test-release"
        assert json_response["executor_protocol_version"] == SUPPORTED_PROTOCOL_VERSION
        assert json_response["concurrency"] == request.concurrency

    @pytest.mark.parametrize(
        ("protocol_version", "aws_managed"),
        [("1", False), (SUPPORTED_PROTOCOL_VERSION, True)],
        ids=["protocol-1-access-key", "protocol-2-managed"],
    )
    async def test_start_benchmark_serializes_committed_dispatch_for_executor_host(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
        protocol_version: str,
        aws_managed: bool,
    ) -> None:
        observed_at_enqueue: dict[str, Any] = {}
        taskiq_message: TaskiqMessage | None = None

        async def capture_message(message: BrokerMessage) -> None:
            nonlocal taskiq_message
            taskiq_message = TaskiqMessage.model_validate(
                main_module.process_benchmark.broker.serializer.loadb(message.message)
            )
            kwargs = taskiq_message.kwargs
            with Session(database_session.get_bind()) as assertion_session:
                benchmark_id = UUID(
                    kwargs["execution_context_json"]["benchmark_id"] if aws_managed else kwargs["benchmark_id_str"]
                )
                dispatch_id = UUID(kwargs["executor_dispatch_id"])
                observed_at_enqueue["benchmark"] = assertion_session.get(Benchmark, benchmark_id)
                observed_at_enqueue["dispatch"] = assertion_session.get(ExecutorDispatch, dispatch_id)
                observed_at_enqueue["tasks"] = assertion_session.exec(
                    select(Task).where(Task.benchmark == benchmark_id)
                ).all()

        active_release = database_session.get(ExecutorRelease, "test-release")
        assert active_release is not None
        active_release.artifact_digest = "a" * 64
        active_release.protocol_version = protocol_version
        database_session.add(active_release)
        database_session.commit()

        if aws_managed:
            monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_ROLE_ORG_IDS", str(TEST_ORG_ID))
            monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_REGION", "deployment-region")
            monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_S3_BUCKET", "deployment-bucket")
            monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_LOG_GROUP", "deployment-log-group")
            monkeypatch.setattr("tracker.config.AWS_DEPLOYMENT_LOG_RETENTION_DAYS", "30")
            monkeypatch.setattr("tracker.config.AWS_MANAGED_SUBMISSIONS_ENABLED", True)
            monkeypatch.setattr(main_module.S3ObjectStore, "exists", AsyncMock(return_value=True))
            request = StartBenchmarkRequest(
                contract=contract,
                benchmark_name="swebench",
                concurrency=1,
                task_ids=["task_0"],
                sandbox_provider="daytona",
                sandbox_provider_secret_name="provider-secret",
            )
        else:
            request = StartBenchmarkRequest(
                contract=contract,
                benchmark_name="swebench",
                concurrency=1,
                task_ids=["task_0"],
                harness_config=harness_config,
            )
        task = main_module.process_benchmark
        monkeypatch.setattr(task, "kicker", lambda: type(task).kicker(task))
        monkeypatch.setattr(task.broker, "kick", capture_message)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

        response = client.post("/start-benchmark", json=request.model_dump())

        assert response.status_code == 200
        assert taskiq_message is not None
        benchmark = observed_at_enqueue["benchmark"]
        dispatch = observed_at_enqueue["dispatch"]
        tasks = observed_at_enqueue["tasks"]
        assert isinstance(benchmark, Benchmark)
        assert benchmark.executor_release_id == "test-release"
        assert benchmark.current_execution_release_id == "test-release"
        assert isinstance(dispatch, ExecutorDispatch)
        assert dispatch.status == ExecutorDispatchStatus.QUEUED
        assert dispatch.benchmark_id == benchmark.id
        assert dispatch.executor_release_id == benchmark.current_execution_release_id
        assert [task.task_id for task in tasks] == ["task_0"]
        assert taskiq_message.args == []
        execution_kwargs = (
            {"execution_context_json"}
            if aws_managed
            else {"start_benchmark_request_json", "benchmark_id_str", "verified_task_ids"}
        )
        assert set(taskiq_message.kwargs) == execution_kwargs | {
            "telemetry_context_json",
            "executor_dispatch_id",
            "executor_release_id",
            "executor_artifact_uri",
            "executor_artifact_digest",
            "executor_protocol_version",
        }

        observed_host: dict[str, object] = {}

        async def capture_dispatch(
            _supervisor: executor_host.ExecutorSupervisor,
            _store: executor_host.ExecutorDispatchStore,
            *,
            executor_dispatch_id: str,
            dispatch: executor_host.ArtifactDispatch,
            process_payload: executor_host.ExecutorProcessPayload,
        ) -> None:
            observed_host.update(
                executor_dispatch_id=executor_dispatch_id,
                dispatch=dispatch,
                process_payload=process_payload,
            )

        monkeypatch.setattr(executor_host, "run_executor_dispatch", capture_dispatch)
        await executor_host.launch_executor.original_func(**taskiq_message.kwargs)

        assert taskiq_message.task_name == executor_host.launch_executor.task_name
        assert STABLE_QUEUE_NAME == executor_host.QUEUE_NAME
        assert taskiq_message.kwargs["executor_protocol_version"] == protocol_version
        assert observed_host["executor_dispatch_id"] == str(dispatch.id)
        process_payload = observed_host["process_payload"]
        assert isinstance(process_payload, executor_host.ExecutorProcessPayload)
        assert process_payload.benchmark_id == str(benchmark.id)
        assert process_payload.verified_task_ids == ["task_0"]
        telemetry_context = taskiq_message.kwargs["telemetry_context_json"]
        assert telemetry_context["request_id"]
        assert isinstance(telemetry_context["trace_headers"], dict)
        child_telemetry_context = cast(
            ExecutorTelemetryContext,
            process_payload.arguments["telemetry_context_json"],
        )
        assert child_telemetry_context["request_id"] == telemetry_context["request_id"]
        assert child_telemetry_context["trace_headers"]
        if aws_managed:
            assert process_payload.arguments == {
                "execution_context_json": taskiq_message.kwargs["execution_context_json"],
                "telemetry_context_json": child_telemetry_context,
            }
        else:
            assert process_payload.arguments == {
                "start_benchmark_request_json": request.model_dump(),
                "benchmark_id_str": str(benchmark.id),
                "verified_task_ids": ["task_0"],
                "telemetry_context_json": child_telemetry_context,
            }
        host_dispatch = observed_host["dispatch"]
        assert isinstance(host_dispatch, executor_host.ArtifactDispatch)
        assert host_dispatch.release_id == dispatch.executor_release_id
        assert host_dispatch.artifact_uri == dispatch.executor_artifact_uri
        assert host_dispatch.artifact_digest == dispatch.executor_artifact_digest
        assert host_dispatch.protocol_version == dispatch.executor_protocol_version

    async def test_start_benchmark_enqueue_failure_is_retryable(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
    ) -> None:
        class FailingKicker:
            async def kiq(self, **_kwargs: Any) -> None:
                raise RuntimeError("redis unavailable")

        failing_kicker = FailingKicker()
        monkeypatch.setattr("main.process_benchmark.kicker", lambda: failing_kicker)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=["task_0"],
            harness_config=harness_config,
        )

        response = client.post("/start-benchmark", json=request.model_dump())

        assert response.status_code == 503
        benchmark_id = UUID(response.json()["detail"]["benchmark_id"])
        benchmark = database_session.get(Benchmark, benchmark_id)
        dispatch = database_session.exec(
            select(ExecutorDispatch).where(ExecutorDispatch.benchmark_id == benchmark_id)
        ).one()
        task = database_session.exec(select(Task).where(Task.benchmark == benchmark_id)).one()
        assert benchmark is not None
        assert benchmark.status == BenchmarkStatus.ERROR
        assert dispatch.status == ExecutorDispatchStatus.FAILED
        assert task.status == TaskStatus.ERROR

    @pytest.mark.parametrize("agent_copy_created", [False, True])
    async def test_start_benchmark_rejects_without_active_executor_release(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: MonkeyPatch,
        agent_copy_created: bool,
    ) -> None:
        admission = database_session.get(ExecutorAdmission, 1)
        assert admission is not None
        database_session.delete(admission)
        database_session.commit()
        created_copy = StoredObjectCopy(deletion_token="copy-version") if agent_copy_created else None
        copy_agent = AsyncMock(return_value=created_copy)
        delete_agent_copy = AsyncMock()
        monkeypatch.setattr("main.copy_agent_to_benchmark", copy_agent)
        monkeypatch.setattr(main_module.S3ObjectStore, "delete", delete_agent_copy)

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=None,
            harness_config=harness_config,
        )
        response = client.post("/start-benchmark", json=request.model_dump())

        assert response.status_code == 503
        expected_detail = "No active executor release is configured"
        assert response.json().get("detail") == expected_detail
        copy_agent.assert_awaited_once()
        if agent_copy_created:
            copy_call = copy_agent.await_args
            assert copy_call is not None
            copied_benchmark_id = copy_call.args[1]
            delete_agent_copy.assert_awaited_once_with(
                f"benchmarks/{copied_benchmark_id}/{contract.name}.zip",
                deletion_token="copy-version",
            )
        else:
            delete_agent_copy.assert_not_awaited()

    async def test_start_payload_failure_rolls_back_and_deletes_created_copy(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        copy_agent = AsyncMock(return_value=StoredObjectCopy(deletion_token="copy-version"))
        delete_agent_copy = AsyncMock()

        def fail_payload_build(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError("payload validation failed")

        monkeypatch.setattr(main_module, "copy_agent_to_benchmark", copy_agent)
        monkeypatch.setattr(main_module.S3ObjectStore, "delete", delete_agent_copy)
        monkeypatch.setattr(main_module, "_process_benchmark_kwargs", fail_payload_build)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=["task_0"],
            harness_config=harness_config,
        )

        response = TestClient(app, raise_server_exceptions=False).post(
            "/start-benchmark",
            json=request.model_dump(),
        )

        assert response.status_code == 500
        assert database_session.exec(select(Benchmark)).all() == []
        assert database_session.exec(select(Task)).all() == []
        assert database_session.exec(select(ExecutorDispatch)).all() == []
        copy_call = copy_agent.await_args
        assert copy_call is not None
        copied_benchmark_id = copy_call.args[1]
        delete_agent_copy.assert_awaited_once_with(
            f"benchmarks/{copied_benchmark_id}/{contract.name}.zip",
            deletion_token="copy-version",
        )

    async def test_start_commit_acknowledgement_failure_retains_durable_copy(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        copy_agent = AsyncMock(return_value=StoredObjectCopy(deletion_token="copy-version"))
        delete_agent_copy = AsyncMock()
        commit = database_session.commit

        def commit_then_fail() -> None:
            commit()
            raise RuntimeError("commit acknowledgement lost")

        monkeypatch.setattr(main_module, "copy_agent_to_benchmark", copy_agent)
        monkeypatch.setattr(main_module.S3ObjectStore, "delete", delete_agent_copy)
        monkeypatch.setattr(database_session, "commit", commit_then_fail)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=["task_0"],
            harness_config=harness_config,
        )

        response = TestClient(app, raise_server_exceptions=False).post(
            "/start-benchmark",
            json=request.model_dump(),
        )

        assert response.status_code == 500
        benchmark = database_session.exec(select(Benchmark)).one()
        dispatch = database_session.exec(select(ExecutorDispatch)).one()
        assert dispatch.benchmark_id == benchmark.id
        assert dispatch.status == ExecutorDispatchStatus.QUEUED
        delete_agent_copy.assert_not_awaited()

    async def test_start_rollback_failure_retains_copy(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        harness_config: HarnessConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        rollback = MagicMock(side_effect=RuntimeError("database connection lost"))
        verify_absent = MagicMock(side_effect=AssertionError("must not read after an uncertain rollback"))
        delete_agent_copy = AsyncMock()
        monkeypatch.setattr(database_session, "rollback", rollback)
        monkeypatch.setattr(main_module, "_start_admission_is_absent", verify_absent)
        monkeypatch.setattr(main_module.S3ObjectStore, "delete", delete_agent_copy)

        await getattr(main_module, "_rollback_failed_start_admission")(
            database_session,
            benchmark_id=uuid4(),
            dispatch_id=uuid4(),
            created_copy=StoredObjectCopy(deletion_token="copy-version"),
            request=StartBenchmarkRequest(
                contract=contract,
                benchmark_name="swebench",
                harness_config=harness_config,
            ),
            object_store=main_module.S3ObjectStore(AWSRuntime.from_harness_config(harness_config)),
        )

        rollback.assert_called_once_with()
        verify_absent.assert_not_called()
        delete_agent_copy.assert_not_awaited()

    async def test_start_benchmark_returns_502_when_benchmark_service_is_unreachable(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=harness_config,
        )

        async def _mock_health_check(*_args: Any, **_kwargs: Any) -> None:
            raise httpx.ConnectError("Name or service not known")

        close_client = AsyncMock(side_effect=RuntimeError("close failed"))
        monkeypatch.setattr(BenchmarkServiceClient, "close", close_client)
        monkeypatch.setattr(BenchmarkServiceClient, "health_check", _mock_health_check)

        no_raise_client = TestClient(app, raise_server_exceptions=False)
        response = no_raise_client.post("/start-benchmark", json=request.model_dump())

        assert response.status_code == 502
        expected_detail = "Benchmark service 'swebench' is not reachable"
        assert response.json().get("detail") == expected_detail
        close_client.assert_awaited_once()

    @pytest.mark.parametrize(
        "custom_benchmark_service",
        [None, "https://swebench.benchmarks.vals.ai:443/path"],
    )
    async def test_start_benchmark_forwards_tracker_api_key_to_benchmark_service(
        self,
        custom_benchmark_service: str | None,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        mock_kicker: Any,
    ) -> None:
        observed_headers: dict[str, str] = {}

        monkeypatch.setattr("tracker.config._BENCHMARK_SERVICE_BASE_URL", "benchmarks.vals.ai")
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=harness_config,
            custom_benchmark_service=custom_benchmark_service,
        )

        async def _mock_health_check(
            service_client: BenchmarkServiceClient, *_args: Any, **_kwargs: Any
        ) -> dict[str, str]:
            observed_headers.update(getattr(service_client, "_headers"))
            return {"status": "ok"}

        async def _mock_verify_task_ids(
            service_client: BenchmarkServiceClient, *_args: Any, **_kwargs: Any
        ) -> VerifyTaskIdsResponse:
            observed_headers.update(getattr(service_client, "_headers"))
            return VerifyTaskIdsResponse(task_ids=["task_0"])

        monkeypatch.setattr(BenchmarkServiceClient, "health_check", _mock_health_check)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)

        response = client.post(
            "/start-benchmark",
            json=request.model_dump(),
            headers={"X-Api-Key": "tracker-api-key"},
        )

        assert response.status_code == 200
        assert observed_headers["X-Descope-Api-Key"] == "tracker-api-key"

        queued_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert queued_request["service_headers"]["X-Descope-Api-Key"] == "tracker-api-key"

    async def test_start_benchmark_does_not_forward_tracker_key_to_custom_service(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        mock_kicker: Any,
    ) -> None:
        observed_headers: dict[str, str] = {}
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            harness_config=harness_config,
            custom_benchmark_service="https://team.example",
        )

        async def _mock_verify_task_ids(
            service_client: BenchmarkServiceClient,
            *_args: Any,
            **_kwargs: Any,
        ) -> VerifyTaskIdsResponse:
            observed_headers.update(getattr(service_client, "_headers"))
            return VerifyTaskIdsResponse(task_ids=["task_0"])

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)

        response = client.post(
            "/start-benchmark",
            json=request.model_dump(),
            headers={"X-Api-Key": "tracker-api-key"},
        )

        assert response.status_code == 200
        assert "X-Descope-Api-Key" not in observed_headers
        queued_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert "X-Descope-Api-Key" not in queued_request["service_headers"]

    async def test_start_benchmark_preserves_custom_service_descope_key(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        mock_kicker: Any,
    ) -> None:
        observed_headers: dict[str, str] = {}
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            harness_config=harness_config,
            custom_benchmark_service="https://team.example",
            service_auth_header_name="X-Descope-Api-Key",
            service_auth_secret_name="TeamBenchmarkKey",
        )

        async def _mock_verify_task_ids(
            service_client: BenchmarkServiceClient,
            *_args: Any,
            **_kwargs: Any,
        ) -> VerifyTaskIdsResponse:
            observed_headers.update(getattr(service_client, "_headers"))
            return VerifyTaskIdsResponse(task_ids=["task_0"])

        def _mock_resolve_secrets(*_args: Any, **_kwargs: Any) -> dict[str, str]:
            return {"X-Descope-Api-Key": "custom-service-key"}

        monkeypatch.setattr(main_module, "resolve_secrets", _mock_resolve_secrets)
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _mock_verify_task_ids)

        response = client.post(
            "/start-benchmark",
            json=request.model_dump(),
            headers={"X-Api-Key": "tracker-api-key"},
        )

        assert response.status_code == 200
        assert observed_headers["X-Descope-Api-Key"] == "custom-service-key"
        queued_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert queued_request["service_headers"]["X-Descope-Api-Key"] == "custom-service-key"

    async def test_start_benchmark_keeps_selected_provider_secret_with_harness_headers(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
        mock_kicker: Any,
    ) -> None:
        """Start requests should keep the provider secret chosen by the client.

        Test cases:
        - Harness headers provide AWS config without a provider secret.
        - The selected provider secret from the request body is stored and queued.
        """
        selected_harness_config = harness_config.model_copy(update={"sandbox_provider_secret_name": "ModalSecrets"})
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=selected_harness_config,
            sandbox_provider="modal",
        )

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

        response = client.post(
            "/start-benchmark",
            json=request.model_dump(),
            headers={
                "x-harness-aws-access-key-id": harness_config.aws.aws_access_key_id,
                "x-harness-aws-secret-access-key": harness_config.aws.aws_secret_access_key,
                "x-harness-aws-default-region": harness_config.aws.aws_default_region,
                "x-harness-s3-bucket": harness_config.s3_bucket,
                "x-harness-log-group": harness_config.log_group,
                "x-harness-log-retention-policy": str(harness_config.log_retention_policy),
            },
        )

        assert response.status_code == 200
        benchmark_row = database_session.get(Benchmark, UUID(response.json()["benchmark_id"]))
        assert benchmark_row
        assert benchmark_row.arguments.sandbox_provider == "modal"
        assert benchmark_row.arguments.sandbox_provider_secret_name == "ModalSecrets"

        queued_request = mock_kicker.queued_calls[0]["start_benchmark_request_json"]
        assert queued_request["sandbox_provider"] == "modal"
        assert queued_request["harness_config"]["sandbox_provider_secret_name"] == "ModalSecrets"

    async def test_fetch_benchmark(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        """Test fetch benchmark of the fastapi server.

        Test Cases:
            - Returns 200 OK
            - Raising exception if benchmark row is not found
            - Existing benchmark without discovered tasks returns empty progress
            - Benchmark details are returned in the response
            - Benchmark details are updated as benchmark progresses
            - Run-level errors are returned only after the benchmark reaches ERROR
        """

        # Test case 1. Return 404 Not Found if benchmark does not exist
        query_params = {"benchmark_id": str(uuid4())}
        response = client.get("/fetch-benchmark", params=query_params, headers=harness_headers)
        assert response.status_code == 404

        # Add benchmark row to the database to fetch
        benchmark_row = example_benchmark_object
        benchmark_row.executor_release_id = "initial-release"
        benchmark_row.current_execution_release_id = "current-release"
        benchmark_row.executor_artifact_uri = "s3://artifacts/initial-release.pex"
        benchmark_row.executor_artifact_digest = "a" * 64
        benchmark_row.executor_protocol_version = "1"

        database_session.add(benchmark_row)
        database_session.commit()

        # Fetch during the interval between benchmark creation and task discovery.
        query_params = {"benchmark_id": str(benchmark_row.id)}
        response = client.get("/fetch-benchmark", params=query_params, headers=harness_headers)

        assert response.status_code == 200

        details = response.json()["details"]
        assert details["total_tasks"] == 0
        assert details["finished_tasks"] == 0
        assert details["task_breakdown"] == {}

        # Push some task rows that we can use to check the progress of the benchmark
        task_rows = [Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id) for i in range(10)]
        database_session.add_all(task_rows)
        database_session.commit()

        # Send request to fetch the benchmark and ensure that the fetch response is returned
        query_params = {"benchmark_id": str(benchmark_row.id)}
        response = client.get("/fetch-benchmark", params=query_params, headers=harness_headers)

        # Test case 2. Returns 200 OK
        assert response.status_code == 200
        details = response.json().get("details")
        assert details

        # Test case 3. Benchmark details are returned in the response
        # NOTE: Let's just check the fields that are being tracked and are due to change
        assert details.get("status") == BenchmarkStatus.IN_PROGRESS
        assert details.get("total_tasks") == 10
        assert details.get("finished_tasks") == 0
        assert response.json().get("error_message") is None
        assert response.json()["executor_release_id"] == "initial-release"
        assert response.json()["current_execution_release_id"] == "current-release"
        assert response.json()["executor_artifact_digest"] == "a" * 64
        assert response.json()["executor_protocol_version"] == "1"

        # Test case 4. Benchmark details are updated as benchmark progresses
        # Change a few to in progress, finished and error
        # NOTE: as of now only error and finished are "task ended" states
        for i, task_row in enumerate(task_rows[:9]):
            if i < 3:
                task_row.status = TaskStatus.IN_PROGRESS
            elif i < 6:
                task_row.status = TaskStatus.FINISHED

                # Create evaluation result rows for finished tasks
                evaluation_result_row = EvaluationResult(
                    org_id=TEST_ORG_ID,
                    task=task_row.id,
                    instance_id=str(uuid4()),
                    result={"finished": True},
                )

                database_session.add(evaluation_result_row)
            else:
                task_row.status = TaskStatus.ERROR
                database_session.add(
                    ErrorResult(
                        org_id=TEST_ORG_ID,
                        task=task_row.id,
                        error_message="Error occurred during task execution or evaluation",
                    )
                )

            database_session.add(task_row)

        database_session.commit()

        # Send request to fetch the benchmark and ensure that the fetch response is returned
        query_params = {"benchmark_id": str(benchmark_row.id)}
        response = client.get("/fetch-benchmark", params=query_params, headers=harness_headers)
        details = response.json().get("details")
        assert details

        # Test case 5. Benchmark details are updated as benchmark progresses
        assert details.get("total_tasks") and details["total_tasks"] == 10
        assert details.get("finished_tasks") and details["finished_tasks"] == 6

        final_evaluation_row = FinalEvaluation(
            org_id=TEST_ORG_ID,
            benchmark=benchmark_row.id,
            final_score=83.25,
            properties={},
        )
        database_session.add(final_evaluation_row)
        database_session.commit()
        database_session.expire_all()

        response = client.get("/fetch-benchmark", params=query_params, headers=harness_headers)

        # Test case 6. Final score is returned when the benchmark has a final evaluation
        assert response.status_code == 200
        assert response.json().get("final_score") == 83.25

        benchmark_row.status = BenchmarkStatus.ERROR
        benchmark_row.error_message = "Dominant task error affecting 10/10 tasks"
        database_session.add(benchmark_row)
        database_session.commit()

        response = client.get("/fetch-benchmark", params=query_params, headers=harness_headers)

        # Test case 7. Terminal errors return the stored run-level message
        assert response.status_code == 200
        assert response.json().get("error_message") == "Dominant task error affecting 10/10 tasks"

    async def test_retrieve_results(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        """Test the retrieve results endpoint of the fastapi server.

        Test Cases:
            - 404 on invalid benchmark id
            - Final evaluation is omitted if benchmark has not finished yet
            - Evaluation results are returned as the tasks are being completed
            - Works when no tasks are completed
            - Base fields are included within response
            - Tasks stopped field is populated when we stop the benchmark
            - Task errors field is populated when we encounter an error
        """

        # Test case 1. 404 on invalid benchmark id
        query_params = {"benchmark_id": str(uuid4())}
        response = client.get("/retrieve-results", params=query_params, headers=harness_headers)
        assert response.status_code == 404

        # Add benchmark row
        benchmark_row = example_benchmark_object
        database_session.add(benchmark_row)
        database_session.commit()

        query_params = {"benchmark_id": str(benchmark_row.id)}
        response = client.get("/retrieve-results", params=query_params, headers=harness_headers)
        assert response.status_code == 200
        response_json = response.json()

        # Base fields are included within response
        assert response_json.get("benchmark_name") == benchmark_row.name
        assert response_json.get("status") == benchmark_row.status

        # Test case 2. Final evaluation is omitted if benchmark has not finished yet
        assert response_json.get("final_evaluation") is None

        # Test case 3. Empty evaluation result are returned if no tasks are completed
        assert response_json.get("evaluation_results") == {}

        # Test case 4. Evaluation results are returned as the tasks are being completed
        task_rows = [
            Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.FINISHED)
            for i in range(10)
        ]
        evaluation_result_rows = [
            EvaluationResult(org_id=TEST_ORG_ID, task=task_row.id, instance_id=str(uuid4()), result={"finished": True})
            for task_row in task_rows
        ]
        database_session.add_all(task_rows)
        database_session.add_all(evaluation_result_rows)
        database_session.commit()

        response = client.get("/retrieve-results", params=query_params, headers=harness_headers)
        assert response.status_code == 200

        # NOTE: We have defaults so we need to exclude none to get the same response as the user
        response_json = FinalViewResponse(**response.json()).model_dump(exclude_none=True)

        # Test case 5. Evaluation results are returned if they exist even if benchmark has not finished yet
        assert response_json.get("evaluation_results")
        assert len(response_json.get("evaluation_results", {})) == 10

        # No stopped tasks or task_errors fields in response
        assert "tasks_stopped" not in response_json
        assert "task_errors" not in response_json

        # Change benchmark status to finished and add final evaluation row
        # Refresh to get the latest state
        database_session.refresh(benchmark_row)
        benchmark_row.status = BenchmarkStatus.FINISHED
        database_session.add(benchmark_row)
        database_session.commit()

        final_evaluation_row = FinalEvaluation(
            org_id=TEST_ORG_ID,
            benchmark=benchmark_row.id,
            final_score=100,
            properties={"resolved_tasks": [], "unresolved_tasks": []},
        )
        database_session.add(final_evaluation_row)
        database_session.commit()

        response = client.get("/retrieve-results", params=query_params, headers=harness_headers)
        assert response.status_code == 200
        response_json = response.json()

        # Test case 6. Final evaluation now exists
        assert response_json.get("final_evaluation")
        assert response_json.get("final_evaluation").get("final_score") == 100

        # Test case 7. Tasks stopped field is populated when we stop the benchmark
        # Stop the benchmark
        benchmark_row.status = BenchmarkStatus.STOPPED
        database_session.add(benchmark_row)
        database_session.commit()

        # Add some new tasks with the status stopped
        task_rows = [
            Task(org_id=TEST_ORG_ID, task_id=f"task_{i}", benchmark=benchmark_row.id, status=TaskStatus.STOPPED)
            for i in range(11, 21)
        ]
        database_session.add_all(task_rows)
        database_session.commit()

        response = client.get("/retrieve-results", params=query_params, headers=harness_headers)
        assert response.status_code == 200
        response_json = response.json()

        # NOTE: We chose to use a number instead of a list or string since some benchmarks have a lot of tasks
        assert response_json.get("tasks_stopped") == 10
        assert len(response_json.get("evaluation_results")) == 10

        # Test case 8. Task errors field is populated when we encounter an error
        # Add some new tasks with the status error (one with ErrorResult and one without)
        error_message = "Error occurred during task execution or evaluation"
        task_rows = [
            Task(
                org_id=TEST_ORG_ID,
                task_id=f"task_{i}",
                benchmark=benchmark_row.id,
                status=TaskStatus.ERROR,
            )
            for i in range(22, 24)
        ]
        database_session.add_all(task_rows)
        database_session.flush()
        database_session.add(
            ErrorResult(
                org_id=TEST_ORG_ID,
                task=task_rows[0].id,
                error_message=error_message,
            )
        )
        database_session.commit()

        response = client.get("/retrieve-results", params=query_params, headers=harness_headers)
        assert response.status_code == 200
        response_json = response.json()

        # Ensure we did not lose any previous data
        assert response_json.get("tasks_stopped") == 10
        assert len(response_json.get("evaluation_results")) == 10
        assert response_json.get("final_evaluation")

        # Check for tasks with error
        assert response_json.get("task_errors")
        assert len(response_json.get("task_errors")) == 2

        # Error message we saved was returned in the response
        assert response_json.get("task_errors").get("task_22") == error_message

        # If we did not get an error message, we return a default message
        assert response_json.get("task_errors").get("task_23") == "No error message was provided"

        # Test case 9. task_ids subset filters evaluation_results and recomputes final_score
        observed_headers: dict[str, str] = {}
        observed_results: dict[str, Any] = {}

        async def _mock_final_score(client: BenchmarkServiceClient, **kwargs: Any) -> FinalScoreResponse:
            observed_headers.update(getattr(client, "_headers"))
            observed_results.clear()
            observed_results.update(kwargs["evaluation_results"])
            ids = list(kwargs["evaluation_results"].keys())
            return FinalScoreResponse(tasks_evaluated=ids, final_score=len(ids), metadata={})

        monkeypatch.setattr(BenchmarkServiceClient, "final_score", _mock_final_score)
        response = client.get(
            "/retrieve-results",
            params=[("benchmark_id", str(benchmark_row.id)), ("task_ids", "task_1"), ("task_ids", "task_3")],
            headers={**harness_headers, "X-Api-Key": "tracker-api-key"},
        )
        assert response.status_code == 200
        body = response.json()
        assert set(body["evaluation_results"]) == {"task_1", "task_3"}
        assert body["final_evaluation"]["final_score"] == 2.0
        assert observed_headers["X-Descope-Api-Key"] == "tracker-api-key"

        # Test case 10. A requested task without a result (stopped/errored/missing) is still
        # scored: it is passed to the benchmark service as {task_id: None}, contributing to
        # the denominator rather than being silently dropped from the subset.
        response = client.get(
            "/retrieve-results",
            params=[("benchmark_id", str(benchmark_row.id)), ("task_ids", "task_1"), ("task_ids", "task_11")],
            headers={**harness_headers, "X-Api-Key": "tracker-api-key"},
        )
        assert response.status_code == 200
        assert observed_results.keys() == {"task_1", "task_11"}
        assert observed_results["task_11"] is None
        assert response.json()["final_evaluation"]["final_score"] == 2.0

    async def test_retrieve_results_blocks_external_persisted_internal_destination(
        self,
        example_benchmark_object: Benchmark,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_headers: dict[str, str],
    ) -> None:
        example_benchmark_object.custom_benchmark_service = "http://service.internal:8001"
        database_session.add(example_benchmark_object)
        database_session.commit()
        monkeypatch.setattr(main_module, "AUTH_REQUIRED", True)

        response = client.get(
            "/retrieve-results",
            params=[
                ("benchmark_id", str(example_benchmark_object.id)),
                ("task_ids", "task_0"),
            ],
            headers=harness_headers,
        )

        assert response.status_code == 403
        assert response.json() == {"detail": "Custom benchmark destination is not allowed"}

    async def test_retrieve_results_does_not_forward_tracker_key_to_legacy_named_custom_service(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        benchmark_row = example_benchmark_object
        benchmark_row.name = "terminal_bench"
        benchmark_row.custom_benchmark_service = "https://team.example"
        task_row = Task(
            org_id=TEST_ORG_ID,
            task_id="task_1",
            benchmark=benchmark_row.id,
            status=TaskStatus.FINISHED,
        )
        database_session.add_all(
            [
                benchmark_row,
                task_row,
                EvaluationResult(
                    org_id=TEST_ORG_ID,
                    task=task_row.id,
                    instance_id=str(uuid4()),
                    result={"finished": True},
                ),
            ]
        )
        database_session.commit()

        observed_headers: dict[str, str] = {}

        async def _mock_final_score(service_client: BenchmarkServiceClient, **kwargs: Any) -> FinalScoreResponse:
            observed_headers.update(getattr(service_client, "_headers"))
            task_ids = list(kwargs["evaluation_results"])
            return FinalScoreResponse(tasks_evaluated=task_ids, final_score=len(task_ids), metadata={})

        monkeypatch.setattr(BenchmarkServiceClient, "final_score", _mock_final_score)
        response = client.get(
            "/retrieve-results",
            params=[("benchmark_id", str(benchmark_row.id)), ("task_ids", "task_1")],
            headers={**harness_headers, "X-Api-Key": "tracker-api-key"},
        )

        assert response.status_code == 200
        assert response.json()["final_evaluation"]["final_score"] == 1.0
        assert "X-Descope-Api-Key" not in observed_headers

    async def test_benchmark_error_handling(
        self,
        contract: AgentContractRequest,
        database_session: Session,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
    ) -> None:
        """Test benchmark error handling of the fastapi server.

        Test Cases:
            - Verify failure returns 502 with the error message
            - No benchmark row is created when pre-flight checks fail
        """

        # Exception is raised if verify task ids fails
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", self._mock_verify_task_ids_error)

        row_count_before = len(database_session.exec(select(Benchmark)).all())

        # Example request sent from the cli to the fastapi server
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=10,
            task_ids=None,
            harness_config=harness_config,
        )

        response = client.post("/start-benchmark", json=request.model_dump())

        # Test case 1. Verify failure returns a stable 502 without internal detail
        assert response.status_code == 502
        assert response.json() == {"detail": "Failed to verify task ids"}
        assert "Error verifying task ids" not in response.text

        # Test case 2. No benchmark row is created when pre-flight checks fail
        row_count_after = len(database_session.exec(select(Benchmark)).all())
        assert row_count_after == row_count_before

    async def test_fetch_benchmarks(self, database_session: Session, example_benchmark_object: Benchmark) -> None:
        """Test fetch benchmarks of the fastapi server.

        Test Cases:
            - Fetch using no filters all all, returns 5 benchmarks with total count
            - can fetch using contract name, benchmark name and status
            - Can order by started at
            - Edge cases with no benchmarks found
        """

        fetch_benchmarks_request = FetchBenchmarksRequest()

        # When no benchmarks have been created yet, we return an empty list and total count of 0
        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("benchmarks") == []
        assert response_json.get("total_count") == 0

        # Add benchmark row to the database to fetch
        example_benchmark_object.executor_release_id = "initial-release"
        example_benchmark_object.current_execution_release_id = "current-release"
        example_benchmark_object.executor_artifact_uri = "s3://artifacts/initial-release.pex"
        example_benchmark_object.executor_artifact_digest = "a" * 64
        example_benchmark_object.executor_protocol_version = "1"
        database_session.add(example_benchmark_object)
        database_session.commit()

        # Add benchmark name to be a random string (expected no matches)
        fetch_benchmarks_request.benchmark_name = [str(uuid4())]

        # When we fetch with no benchmarks found, we return an empty list and total count of 0
        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("benchmarks") == []
        assert response_json.get("total_count") == 0

        # Create 4 more benchmark rows that have the same data
        benchmark_rows = [
            Benchmark(org_id=TEST_ORG_ID, id=uuid4(), name="swebench", arguments=example_benchmark_object.arguments)
            for _ in range(4)
        ]
        for benchmark_row in benchmark_rows:
            database_session.add(benchmark_row)
            database_session.commit()

        # Create benchmark with unique data
        unique_contract = AgentContractRequest(
            name="terminus_2",
            install_cmd="echo installing dependencies...",
            run_cmd="echo running agent...",
        )
        unique_benchmark = Benchmark(
            org_id=TEST_ORG_ID,
            name="terminal_bench",
            arguments=BenchmarkArguments(
                contract=unique_contract,
                concurrency=5,
                task_ids=None,
                slice_str=None,
                dataset="terminal-bench-2.1",
            ),
        )
        database_session.add(unique_benchmark)
        database_session.commit()

        # Search for the 4 benchmarks just created + the original one we added before
        fetch_benchmarks_request.benchmark_name = ["swebench"]
        fetch_benchmarks_request.agent_name = ["dummy"]
        fetch_benchmarks_request.status = [BenchmarkStatus.IN_PROGRESS]

        # When we fetch with benchmarks found, we return a 200 OK
        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("total_count") == 5
        assert len(response_json.get("benchmarks")) == 5

        expected_fields = set(BenchmarkTableRow.model_fields.keys())
        for row in response_json["benchmarks"]:
            assert set(row.keys()) == expected_fields

        persisted_row = next(
            row for row in response_json["benchmarks"] if row["id"] == str(example_benchmark_object.id)
        )
        assert persisted_row["executor_release_id"] == "initial-release"
        assert persisted_row["current_execution_release_id"] == "current-release"
        assert persisted_row["executor_artifact_digest"] == "a" * 64
        assert persisted_row["executor_protocol_version"] == "1"

        # Clear filters and search again (checking limit and total)
        fetch_benchmarks_request.benchmark_name = None  # type: ignore[assignment]
        fetch_benchmarks_request.agent_name = None  # type: ignore[assignment]
        fetch_benchmarks_request.status = None  # type: ignore[assignment]

        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()

        # There are 6 total benchmarks (default limit is 50, so all are returned)
        assert response_json.get("total_count") == 6
        assert len(response_json.get("benchmarks")) == 6

        # Change benchmark status to finished and search again
        unique_benchmark.status = BenchmarkStatus.FINISHED
        database_session.add(unique_benchmark)
        database_session.commit()

        # Search for finished benchmarks
        fetch_benchmarks_request.status = [BenchmarkStatus.FINISHED]

        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()

        # There is 1 finished benchmark
        assert response_json.get("total_count") == 1
        assert len(response_json.get("benchmarks")) == 1

        example_benchmark_object.status = BenchmarkStatus.ERROR
        example_benchmark_object.error_message = "Dominant task error"
        database_session.add(example_benchmark_object)
        database_session.commit()

        fetch_benchmarks_request.status = [BenchmarkStatus.ERROR]
        response = client.get(
            "/fetch-benchmarks", params=fetch_benchmarks_request.model_dump(exclude_none=True, mode="json")
        )
        assert response.status_code == 200
        response_json = response.json()
        assert response_json.get("total_count") == 1
        assert response_json["benchmarks"][0]["error_message"] == "Dominant task error"

    async def test_start_benchmark_blocks_external_internal_custom_destination(
        self,
        contract: AgentContractRequest,
        harness_config: HarnessConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(main_module, "AUTH_REQUIRED", True)
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=5,
            task_ids=None,
            harness_config=harness_config,
            custom_benchmark_service="http://10.0.0.1:8001",
        )

        response = client.post("/start-benchmark", json=request.model_dump())

        assert response.status_code == 403
        assert response.json() == {"detail": "Custom benchmark destination is not allowed"}

    async def test_start_benchmark_accepts_custom_service_from_request(
        self,
        contract: AgentContractRequest,
        harness_config: HarnessConfig,
    ) -> None:
        allowed_url = "http://internal-swebench.example.com:8001"
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=5,
            task_ids=None,
            harness_config=harness_config,
            custom_benchmark_service=allowed_url,
        )

        response = client.post("/start-benchmark", json=request.model_dump())

        assert response.status_code == 200

    async def test_fetch_benchmarks_filters_by_dataset(
        self, database_session: Session, contract: AgentContractRequest
    ) -> None:
        benchmark_rows = [
            Benchmark(
                org_id=TEST_ORG_ID,
                name="terminal-bench",
                arguments=BenchmarkArguments(contract=contract, concurrency=1, dataset=None),
            ),
            Benchmark(
                org_id=TEST_ORG_ID,
                name="terminal-bench",
                arguments=BenchmarkArguments(contract=contract, concurrency=1, dataset="default"),
            ),
            Benchmark(
                org_id=TEST_ORG_ID,
                name="terminal-bench",
                arguments=BenchmarkArguments(contract=contract, concurrency=1, dataset="terminal-bench-2.1"),
            ),
        ]
        database_session.add_all(benchmark_rows)
        database_session.commit()

        response = client.get("/fetch-benchmarks", params={"dataset": "terminal-bench-2.1", "limit": 10})

        assert response.status_code == 200
        response_json = response.json()
        assert response_json["total_count"] == 1
        assert response_json["benchmarks"][0]["dataset"] == "terminal-bench-2.1"

        response = client.get("/fetch-benchmarks", params={"dataset": "default", "limit": 10})

        assert response.status_code == 200
        response_json = response.json()
        assert response_json["total_count"] == 2
        assert {row["dataset"] for row in response_json["benchmarks"]} == {"default"}

    async def test_start_benchmark_writes_started_by_columns(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
    ) -> None:
        test_org = Org(id=TEST_ORG_ID, name="default")
        app.dependency_overrides[get_current_starter] = lambda: RequestIdentity(
            org=test_org,
            access_key_id="K2abc",
            email="alice@vals.ai",
            name="Alice",
        )

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=None,
            harness_config=harness_config,
        )
        response = client.post("/start-benchmark", json=request.model_dump())

        assert response.status_code == 200
        benchmark_id = UUID(response.json()["benchmark_id"])

        benchmark_row = database_session.get(Benchmark, benchmark_id)
        assert benchmark_row is not None
        assert benchmark_row.started_by_id == "K2abc"
        assert benchmark_row.started_by_email == "alice@vals.ai"

    async def test_start_benchmark_self_hosted_writes_nulls(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
    ) -> None:
        # The autouse override_starter fixture already returns a self-hosted identity.
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=None,
            harness_config=harness_config,
        )
        response = client.post("/start-benchmark", json=request.model_dump())
        assert response.status_code == 200

        benchmark_id = UUID(response.json()["benchmark_id"])
        benchmark_row = database_session.get(Benchmark, benchmark_id)
        assert benchmark_row is not None
        assert benchmark_row.started_by_id is None
        assert benchmark_row.started_by_email is None

    async def test_start_benchmark_warns_when_email_claim_missing(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        harness_config: HarnessConfig,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Hosted-mode start with an email-less access key emits a one-shot warning. Self-hosted
        identity (access_key_id is None) must NOT warn — that's the bug fix for the warning
        firing on every authenticated request.
        """
        test_org = Org(id=TEST_ORG_ID, name="default")
        app.dependency_overrides[get_current_starter] = lambda: RequestIdentity(
            org=test_org,
            access_key_id="K2abc",
            email=None,
            name=None,
        )

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=None,
            harness_config=harness_config,
        )
        with caplog.at_level(logging.WARNING, logger="main"):
            response = client.post("/start-benchmark", json=request.model_dump())
        assert response.status_code == 200
        benchmark_id = response.json()["benchmark_id"]

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "email" in r.message]
        assert len(warnings) == 1
        assert "K2abc" in warnings[0].message
        assert getattr(warnings[0], "benchmark_id") == benchmark_id

    async def test_init_org_returns_email_claim_present(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        monkeypatch.setattr("main.AUTH_REQUIRED", True)

        mock_client = MagicMock(spec=DescopeClient)
        mock_client.exchange_access_key.return_value = {
            "tenants": {"test-tenant": {}},
            "keyId": "K2abc",
            "sessionToken": {
                "sub": "K2abc",
                "tenants": {"test-tenant": {}},
                "email": "alice@vals.ai",
                "name": "Alice",
            },
        }
        monkeypatch.setattr("tracker.auth._descope_client", mock_client)

        response = client.post("/init", headers={"X-Api-Key": "valid-key"})
        assert response.status_code == 200
        body = response.json()
        assert not body["email_claim_missing"]
        assert body["org_name"] == "test-tenant"

    async def test_init_org_returns_email_claim_missing(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        monkeypatch.setattr("main.AUTH_REQUIRED", True)

        mock_client = MagicMock(spec=DescopeClient)
        mock_client.exchange_access_key.return_value = {
            "tenants": {"test-tenant": {}},
            "keyId": "K2abc",
            "sessionToken": {
                "sub": "K2abc",
                "tenants": {"test-tenant": {}},
            },
        }
        monkeypatch.setattr("tracker.auth._descope_client", mock_client)

        response = client.post("/init", headers={"X-Api-Key": "valid-key"})
        assert response.status_code == 200
        assert response.json()["email_claim_missing"]

    async def test_init_org_uses_bound_user_email_when_email_claim_missing(
        self,
        monkeypatch: MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("tracker.auth.AUTH_REQUIRED", True)
        monkeypatch.setattr("main.AUTH_REQUIRED", True)

        mock_client = MagicMock(spec=DescopeClient)
        mock_client.exchange_access_key.return_value = {
            "tenants": {"test-tenant": {}},
            "keyId": "K2abc",
            "sessionToken": {
                "sub": "K2abc",
                "tenants": {"test-tenant": {}},
                "customClaims": {"user_id": "U2abc"},
            },
        }
        mock_client.mgmt.user.load_by_user_id.return_value = {
            "user": {
                "email": "alice@vals.ai",
                "displayName": "Alice",
            },
        }
        monkeypatch.setattr("tracker.auth._descope_client", mock_client)

        response = client.post("/init", headers={"X-Api-Key": "valid-key"})
        assert response.status_code == 200
        assert not response.json()["email_claim_missing"]
        mock_client.mgmt.user.load_by_user_id.assert_called_once_with("U2abc")

    async def test_fetch_benchmarks_includes_started_by_email(
        self,
        contract: AgentContractRequest,
        database_session: Session,
    ) -> None:
        bench = Benchmark(
            org_id=TEST_ORG_ID,
            name="swebench",
            arguments=BenchmarkArguments(contract=contract, concurrency=1),
            started_by_email="alice@vals.ai",
            started_by_id="K2abc",
        )
        database_session.add(bench)
        database_session.commit()

        response = client.get("/fetch-benchmarks", params={"limit": 10, "offset": 0})
        assert response.status_code == 200

        rows = response.json()["benchmarks"]
        assert any(r["started_by_email"] == "alice@vals.ai" for r in rows)

    async def test_fetch_benchmarks_started_by_query_param_filters(
        self,
        contract: AgentContractRequest,
        database_session: Session,
    ) -> None:
        """Regression: FastAPI's Depends(PydanticModel) doesn't bind list[str] from query params;
        started_by must be declared as a separate Query() parameter on the endpoint.
        """
        for email in ("alice@vals.ai", "bob@vals.ai", None):
            database_session.add(
                Benchmark(
                    org_id=TEST_ORG_ID,
                    name="swebench",
                    arguments=BenchmarkArguments(contract=contract, concurrency=1),
                    started_by_email=email,
                    started_by_id=f"K-{email or 'none'}",
                )
            )
        database_session.commit()

        response = client.get("/fetch-benchmarks", params={"started_by": "alice@vals.ai", "limit": 10})
        assert response.status_code == 200
        rows = response.json()["benchmarks"]
        assert len(rows) == 1
        assert rows[0]["started_by_email"] == "alice@vals.ai"

        # Repeated query params for multi-value filter
        response = client.get(
            "/fetch-benchmarks",
            params=[("started_by", "alice@vals.ai"), ("started_by", "bob@vals.ai"), ("limit", "10")],
        )
        assert response.status_code == 200
        rows = response.json()["benchmarks"]
        assert len(rows) == 2
        assert {r["started_by_email"] for r in rows} == {"alice@vals.ai", "bob@vals.ai"}

    async def test_run_label_is_persisted_fetchable_and_filterable(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
        harness_headers: dict[str, str],
    ) -> None:
        """Run labels should persist on start and be visible through fetch and list.

        Test cases:
            - Start stores the label on the benchmark row.
            - Fetch and list responses expose the label, and list can filter by it.
        """

        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _verify_single_task_id)

        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=None,
            harness_config=harness_config,
            label="nightly",
        )
        start_response = client.post("/start-benchmark", json=request.model_dump())
        assert start_response.status_code == 200
        benchmark_id = UUID(start_response.json()["benchmark_id"])

        benchmark_row = database_session.get(Benchmark, benchmark_id)
        assert benchmark_row is not None
        assert benchmark_row.label == "nightly"

        fetch_response = client.get(
            "/fetch-benchmark",
            params={"benchmark_id": str(benchmark_id)},
            headers=harness_headers,
        )
        assert fetch_response.status_code == 200
        assert fetch_response.json()["label"] == "nightly"

        list_response = client.get("/fetch-benchmarks", params={"label": "nightly", "limit": 10})
        assert list_response.status_code == 200
        rows = list_response.json()["benchmarks"]
        assert len(rows) == 1
        assert rows[0]["id"] == str(benchmark_id)
        assert rows[0]["label"] == "nightly"

    async def test_fetch_benchmark_metadata_includes_started_by_email(
        self,
        contract: AgentContractRequest,
        database_session: Session,
    ) -> None:
        bench = Benchmark(
            org_id=TEST_ORG_ID,
            name="swebench",
            arguments=BenchmarkArguments(contract=contract, concurrency=1),
            started_by_email="alice@vals.ai",
            started_by_id="K2abc",
        )
        database_session.add(bench)
        database_session.commit()

        response = client.get(f"/fetch-benchmark-metadata/{bench.id}")
        assert response.status_code == 200
        assert response.json()["started_by_email"] == "alice@vals.ai"

    async def test_fetch_run_outputs_streams_tar(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        observed_prefixes: list[str] = []

        async def _mock_list_objects(_store: object, prefix: str) -> AsyncIterator[StoredObject]:
            observed_prefixes.append(prefix)
            yield StoredObject(key=f"{prefix}output.txt")

        async def _mock_get_many(_store: object, keys: AsyncIterator[str]) -> AsyncIterator[tuple[str, bytes]]:
            async for key in keys:
                yield key, b"output contents"

        monkeypatch.setattr(main_module.S3ObjectStore, "list_objects", _mock_list_objects)
        monkeypatch.setattr(main_module.S3ObjectStore, "get_many", _mock_get_many)

        response = client.get(
            f"/fetch-run-outputs/{example_benchmark_object.id}",
            params={"task_ids": ["task_1", "task_2"]},
            headers=harness_headers,
        )

        assert response.status_code == 200
        assert response.headers.get("content-type") == "application/x-tar"
        assert response.headers.get("content-disposition") == (
            f"attachment; filename=benchmark_{example_benchmark_object.id}_outputs.tar"
        )
        assert observed_prefixes == [
            f"benchmarks/{example_benchmark_object.id}/task_1/",
            f"benchmarks/{example_benchmark_object.id}/task_2/",
        ]

        with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:") as tar:
            assert tar.getnames() == ["task_1/output.txt", "task_2/output.txt"]
            task_file = tar.extractfile("task_1/output.txt")
            assert task_file is not None
            assert task_file.read() == b"output contents"

    async def test_fetch_run_outputs_omits_unsafe_tar_members(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        """Never copy unsafe S3 key suffixes into a downloaded tar."""
        database_session.add(example_benchmark_object)
        database_session.commit()

        async def _mock_list_objects(_store: object, prefix: str) -> AsyncIterator[StoredObject]:
            yield StoredObject(key=f"{prefix}task/../../outside.txt")
            yield StoredObject(key=f"{prefix}task/..\\outside.txt")
            yield StoredObject(key=f"{prefix}C:/outside.txt")
            yield StoredObject(key=f"{prefix}task//outside.txt")
            yield StoredObject(key=f"{prefix}task/hidden\x00.txt")
            yield StoredObject(key=f"{prefix}task/output.txt")

        async def _mock_get_many(_store: object, keys: AsyncIterator[str]) -> AsyncIterator[tuple[str, bytes]]:
            async for key in keys:
                yield key, b"output contents"

        monkeypatch.setattr(main_module.S3ObjectStore, "list_objects", _mock_list_objects)
        monkeypatch.setattr(main_module.S3ObjectStore, "get_many", _mock_get_many)

        response = client.get(
            f"/fetch-run-outputs/{example_benchmark_object.id}",
            headers=harness_headers,
        )

        assert response.status_code == 200
        with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:") as tar:
            assert tar.getnames() == ["task/output.txt"]

    async def test_fetch_run_outputs_returns_404_when_all_members_are_unsafe(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        """Do not download outputs when every listed tar member name is unsafe."""
        database_session.add(example_benchmark_object)
        database_session.commit()

        async def _mock_list_objects(_store: object, prefix: str) -> AsyncIterator[StoredObject]:
            yield StoredObject(key=f"{prefix}task/../../outside.txt")
            yield StoredObject(key=f"{prefix}task/hidden\x00.txt")

        get_many = MagicMock()
        monkeypatch.setattr(main_module.S3ObjectStore, "list_objects", _mock_list_objects)
        monkeypatch.setattr(main_module.S3ObjectStore, "get_many", get_many)

        response = client.get(
            f"/fetch-run-outputs/{example_benchmark_object.id}",
            headers=harness_headers,
        )

        assert response.status_code == 404
        assert response.json() == {"detail": f"No outputs found for run '{example_benchmark_object.id}'"}
        get_many.assert_not_called()

    async def test_fetch_run_outputs_returns_404_when_empty(
        self,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        example_benchmark_object: Benchmark,
        harness_headers: dict[str, str],
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        def _mock_list_objects(_store: object, _prefix: str) -> AsyncIterator[StoredObject]:
            return async_iterator(())

        monkeypatch.setattr(main_module.S3ObjectStore, "list_objects", _mock_list_objects)

        response = client.get(
            f"/fetch-run-outputs/{example_benchmark_object.id}",
            headers=harness_headers,
        )

        assert response.status_code == 404
        assert response.json() == {"detail": f"No outputs found for run '{example_benchmark_object.id}'"}

    async def test_benchmark_service_unauthenticated_error_returns(
        self,
        contract: AgentContractRequest,
        monkeypatch: MonkeyPatch,
        database_session: Session,
        harness_config: HarnessConfig,
        harness_headers: dict[str, str],
    ) -> None:
        """Test that BenchmarkServiceUnauthenticatedError returns 502 without capturing to Sentry.

        Test Cases:
            - /start-benchmark returns 502 when benchmark service returns 401
            - /fetch-benchmark-tasks returns 502 when benchmark service returns 401
            - /retry-or-resume-benchmark returns 502 when benchmark service returns 401
            - None of the above cases capture the exception to Sentry
        """
        captured: list[Exception] = []

        def capture_exception(exception: Exception) -> None:
            captured.append(exception)

        monkeypatch.setattr("sentry_sdk.capture_exception", capture_exception)

        async def _raise_unauth(*_args: Any, **_kwargs: Any) -> VerifyTaskIdsResponse:
            raise BenchmarkServiceUnauthenticatedError("401 Unauthorized")

        no_raise_client = TestClient(app, raise_server_exceptions=False)

        # Case 1: /start-benchmark
        monkeypatch.setattr(BenchmarkServiceClient, "verify_task_ids", _raise_unauth)
        request = StartBenchmarkRequest(
            contract=contract,
            benchmark_name="swebench",
            concurrency=1,
            task_ids=None,
            harness_config=harness_config,
        )

        response = no_raise_client.post("/start-benchmark", json=request.model_dump())
        assert response.status_code == 502
        assert response.json() == {"detail": "Benchmark service authentication failed"}
        assert "401 Unauthorized" not in response.text

        # Case 2: /fetch-benchmark-tasks
        response = no_raise_client.post(
            "/fetch-benchmark-tasks",
            json={"benchmark_name": "swebench"},
        )
        assert response.status_code == 502
        assert response.json() == {"detail": "Failed to fetch task ids from benchmark service"}
        assert "401 Unauthorized" not in response.text

        # Case 3: /retry-or-resume-benchmark — needs at least one task so verify_task_ids is reached
        benchmark = Benchmark(
            org_id=TEST_ORG_ID,
            name="swebench",
            status=BenchmarkStatus.ERROR,
            executor_release_id="test-release",
            executor_artifact_uri="s3://artifacts/test-release.pex",
            executor_artifact_digest="digest-test-release",
            executor_protocol_version="1",
            arguments=BenchmarkArguments(contract=contract, concurrency=1),
        )
        database_session.add(benchmark)
        database_session.commit()
        database_session.add(
            Task(org_id=TEST_ORG_ID, task_id="task_0", benchmark=benchmark.id, status=TaskStatus.ERROR)
        )
        database_session.commit()

        response = no_raise_client.post(
            f"/retry-or-resume-benchmark/{benchmark.id}",
            json={"task_ids": [], "service_headers": {}},
            params={"retry": "true"},
            headers=harness_headers,
        )
        assert response.status_code == 502
        assert response.json() == {"detail": "Benchmark service authentication failed"}
        assert "401 Unauthorized" not in response.text

        # None of the three cases should have reached Sentry
        assert captured == []

    def test_fetch_access_key_run_without_headers_explains_legacy_recovery(
        self,
        database_session: Session,
        example_benchmark_object: Benchmark,
    ) -> None:
        database_session.add(example_benchmark_object)
        database_session.commit()

        response = client.get("/fetch-benchmark", params={"benchmark_id": str(example_benchmark_object.id)})

        assert response.status_code == 400
        assert response.json() == {
            "detail": "This run was started with access-key AWS and requires its legacy AWS configuration."
        }
